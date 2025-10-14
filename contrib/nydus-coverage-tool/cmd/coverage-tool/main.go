package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	githubModelsAPIURL = "https://models.inference.ai.azure.com/chat/completions"
)

// CoverageData represents the JSON output from cargo llvm-cov
type CoverageData struct {
	Data []struct {
		Files []FileData `json:"files"`
	} `json:"data"`
}

// FileData represents coverage data for a single file
type FileData struct {
	Filename string `json:"filename"`
	Summary  struct {
		Lines struct {
			Covered int `json:"covered"`
			Count   int `json:"count"`
		} `json:"lines"`
		Regions struct {
			Covered int `json:"covered"`
			Count   int `json:"count"`
		} `json:"regions"`
		Functions struct {
			Covered int `json:"covered"`
			Count   int `json:"count"`
		} `json:"functions"`
	} `json:"summary"`
}

// FileStats represents coverage statistics for a file
type FileStats struct {
	Filename  string
	Coverage  float64
	Covered   int
	Total     int
	Functions map[string]int
	Regions   map[string]int
}

// CoverageAnalysis represents the result of coverage analysis
type CoverageAnalysis struct {
	File  string                 `json:"file"`
	Stats map[string]interface{} `json:"stats"`
}

// OverallCoverage represents overall project coverage
type OverallCoverage struct {
	TotalFiles      int     `json:"total_files"`
	AverageCoverage float64 `json:"average_coverage"`
}

var (
	rootCmd = &cobra.Command{
		Use:   "coverage-tool",
		Short: "Automated coverage improvement tool for Nydus",
		Long:  `A tool to analyze coverage, generate tests using AI, and create coverage reports.`,
	}

	analyzeCmd = &cobra.Command{
		Use:   "analyze",
		Short: "Analyze current test coverage",
		RunE:  runAnalyze,
	}

	generateCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate unit tests using AI",
		RunE:  runGenerate,
	}

	validateCmd = &cobra.Command{
		Use:   "validate",
		Short: "Validate generated tests",
		RunE:  runValidate,
	}

	reportCmd = &cobra.Command{
		Use:   "report",
		Short: "Generate coverage improvement report",
		RunE:  runReport,
	}

	outputDir  string
	maxRetries int
	modelName  string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&outputDir, "output-dir", "/tmp", "Directory for output files")
	validateCmd.Flags().IntVar(&maxRetries, "max-retries", 3, "Maximum validation retry attempts")
	generateCmd.Flags().StringVar(&modelName, "model", "gpt-4.1-mini", "AI model to use for test generation")
	validateCmd.Flags().StringVar(&modelName, "model", "gpt-4.1-mini", "AI model to use for test generation")

	rootCmd.AddCommand(analyzeCmd)
	rootCmd.AddCommand(generateCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(reportCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runAnalyze(cmd *cobra.Command, args []string) error {
	fmt.Println("================================================================================")
	fmt.Println("Coverage Analysis")
	fmt.Println("================================================================================")

	// Run cargo llvm-cov
	coverageData, err := runCoverage()
	if err != nil {
		return fmt.Errorf("failed to run coverage: %w", err)
	}

	// Extract file-level coverage
	fileStats := extractFileCoverage(coverageData)
	if len(fileStats) == 0 {
		return fmt.Errorf("no files found for coverage analysis")
	}

	// Sort by coverage percentage (ascending)
	sort.Slice(fileStats, func(i, j int) bool {
		return fileStats[i].Coverage < fileStats[j].Coverage
	})

	// Print summary
	fmt.Printf("\nFound %d files with coverage data\n", len(fileStats))
	fmt.Println("\nTop 10 least covered files:")
	fmt.Println("--------------------------------------------------------------------------------")

	for i := 0; i < 10 && i < len(fileStats); i++ {
		fs := fileStats[i]
		fmt.Printf("%2d. %5.2f%% - %s\n", i+1, fs.Coverage, fs.Filename)
		fmt.Printf("    Lines: %d/%d\n", fs.Covered, fs.Total)
	}

	// Select one random least covered file
	leastCovered := fileStats[rand.Intn(len(fileStats)/10+1)]

	fmt.Println("\n================================================================================")
	fmt.Println("Least covered file selected for improvement:")
	fmt.Println("================================================================================")
	fmt.Printf("File: %s\n", leastCovered.Filename)
	fmt.Printf("Coverage: %.2f%%\n", leastCovered.Coverage)
	fmt.Printf("Lines covered: %d/%d\n", leastCovered.Covered, leastCovered.Total)
	fmt.Printf("Functions: %d/%d\n", leastCovered.Functions["covered"], leastCovered.Functions["count"])

	// Save results
	analysis := CoverageAnalysis{
		File: leastCovered.Filename,
		Stats: map[string]interface{}{
			"coverage":  leastCovered.Coverage,
			"covered":   leastCovered.Covered,
			"total":     leastCovered.Total,
			"functions": leastCovered.Functions,
			"regions":   leastCovered.Regions,
		},
	}

	analysisPath := filepath.Join(outputDir, "coverage_analysis.json")
	if err := saveJSON(analysisPath, analysis); err != nil {
		return fmt.Errorf("failed to save analysis: %w", err)
	}
	fmt.Printf("\nResults written to %s\n", analysisPath)

	// Save overall coverage
	totalCoverage := 0.0
	for _, fs := range fileStats {
		totalCoverage += fs.Coverage
	}

	overall := OverallCoverage{
		TotalFiles:      len(fileStats),
		AverageCoverage: totalCoverage / float64(len(fileStats)),
	}

	overallPath := filepath.Join(outputDir, "overall_coverage.json")
	if err := saveJSON(overallPath, overall); err != nil {
		return fmt.Errorf("failed to save overall coverage: %w", err)
	}
	fmt.Printf("Overall stats written to %s\n", overallPath)

	return nil
}

func generateTests() error {
	fmt.Println("================================================================================")
	fmt.Println("Generating Unit Tests")
	fmt.Println("================================================================================")

	// Read coverage analysis
	analysisPath := filepath.Join(outputDir, "coverage_analysis.json")
	var analysis CoverageAnalysis
	if err := loadJSON(analysisPath, &analysis); err != nil {
		return fmt.Errorf("failed to load coverage analysis: %w", err)
	}

	filename := analysis.File
	coverage := analysis.Stats["coverage"].(float64)

	fmt.Printf("Target file: %s\n", filename)
	fmt.Printf("Current coverage: %.2f%%\n\n", coverage)

	// Read target file content
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Collect module context (all files in the same module)
	moduleFiles, err := collectModuleFiles(filename)
	if err != nil {
		return fmt.Errorf("failed to collect module files: %w", err)
	}

	fmt.Printf("Collected %d files from the module for context\n\n", len(moduleFiles))

	// Generate tests using GitHub Models API
	generatedTests, err := callGitHubModelsAPI(string(content), filename, analysis.Stats, moduleFiles, modelName)
	if err != nil {
		return fmt.Errorf("failed to generate tests: %w", err)
	}

	fmt.Println("\nGenerated tests:")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(generatedTests)
	fmt.Println("--------------------------------------------------------------------------------")

	// Integrate tests into the file
	updatedContent := integrateTests(string(content), generatedTests)

	// Save updated file
	outputPath := filepath.Join(outputDir, "updated_file.rs")
	if err := os.WriteFile(outputPath, []byte(updatedContent), 0644); err != nil {
		return fmt.Errorf("failed to write updated file: %w", err)
	}
	fmt.Printf("\nUpdated file saved to %s\n", outputPath)

	// Save metadata
	metadata := map[string]interface{}{
		"original_file":        filename,
		"generated_tests_path": outputPath,
		"coverage_before":      coverage,
		"model":                modelName,
	}

	metadataPath := filepath.Join(outputDir, "test_generation_metadata.json")
	if err := saveJSON(metadataPath, metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}
	fmt.Printf("Metadata saved to %s\n", metadataPath)

	return nil
}

func runGenerate(cmd *cobra.Command, args []string) error {
	return generateTests()
}

func runValidate(cmd *cobra.Command, args []string) error {
	fmt.Println("================================================================================")
	fmt.Println("Validating Generated Tests")
	fmt.Println("================================================================================")

	// Read metadata
	metadataPath := filepath.Join(outputDir, "test_generation_metadata.json")
	var metadata map[string]interface{}
	if err := loadJSON(metadataPath, &metadata); err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	originalFilePath := metadata["original_file"].(string)

	// Use model from metadata if available and not overridden by CLI flag
	if savedModel, ok := metadata["model"].(string); ok && modelName == "gpt-4.1-mini" {
		modelName = savedModel
	}

	// Read coverage analysis for regeneration
	analysisPath := filepath.Join(outputDir, "coverage_analysis.json")
	var analysis CoverageAnalysis
	if err := loadJSON(analysisPath, &analysis); err != nil {
		return fmt.Errorf("failed to load coverage analysis: %w", err)
	}

	// Create backup
	backupPath := originalFilePath + ".backup"
	if err := copyFile(originalFilePath, backupPath); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}
	defer os.Remove(backupPath)

	success := false
	var lastErr error
	actualAttempts := 0

	for attempt := 1; attempt <= maxRetries; attempt++ {
		actualAttempts = attempt
		fmt.Printf("\n================================================================================\n")
		fmt.Printf("Validation Attempt %d/%d\n", attempt, maxRetries)
		fmt.Printf("================================================================================\n")

		testFilePath := metadata["generated_tests_path"].(string)

		// Copy generated file to original location
		if err := copyFile(testFilePath, originalFilePath); err != nil {
			lastErr = err
			continue
		}

		// Run cargo check
		fmt.Println("\nRunning cargo check...")
		if err := runCommand("make", "ut"); err != nil {
			fmt.Println("❌ UT failed!")
			lastErr = err
			copyFile(backupPath, originalFilePath) // Restore backup
			// Regenerate tests
			if err := generateTests(); err != nil {
				return fmt.Errorf("failed to regenerate tests: %w", err)
			}
			continue
		}
		fmt.Println("✅ UT successful!")

		success = true
		break
	}

	// Update metadata
	metadata["validation_success"] = success
	metadata["validation_attempts"] = actualAttempts

	if err := saveJSON(metadataPath, metadata); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	if success {
		fmt.Println("\n================================================================================")
		fmt.Println("✅ SUCCESS: Tests validated and applied!")
		fmt.Println("================================================================================")
		return nil
	}

	// Restore backup on failure
	copyFile(backupPath, originalFilePath)
	fmt.Println("\n================================================================================")
	fmt.Println("❌ FAILURE: All validation attempts failed")
	fmt.Println("================================================================================")
	return fmt.Errorf("validation failed after %d attempts: %w", actualAttempts, lastErr)
}

func runReport(cmd *cobra.Command, args []string) error {
	fmt.Println("================================================================================")
	fmt.Println("Generating Coverage Improvement Report")
	fmt.Println("================================================================================")

	// Read metadata
	metadataPath := filepath.Join(outputDir, "test_generation_metadata.json")
	var metadata map[string]interface{}
	if err := loadJSON(metadataPath, &metadata); err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// Read before analysis
	beforeAnalysisPath := filepath.Join(outputDir, "coverage_analysis.json")
	var beforeAnalysis CoverageAnalysis
	if err := loadJSON(beforeAnalysisPath, &beforeAnalysis); err != nil {
		return fmt.Errorf("failed to load before analysis: %w", err)
	}

	beforeOverallPath := filepath.Join(outputDir, "overall_coverage.json")
	var beforeOverall OverallCoverage
	if err := loadJSON(beforeOverallPath, &beforeOverall); err != nil {
		return fmt.Errorf("failed to load before overall: %w", err)
	}

	// Run coverage after improvements
	fmt.Println("Running post-improvement coverage analysis...")
	afterCoverageData, err := runCoverage()
	if err != nil {
		return fmt.Errorf("failed to run after coverage: %w", err)
	}

	// Extract coverage for target file
	targetFile := metadata["original_file"].(string)
	afterFileStats := extractFileCoverage(afterCoverageData)

	var afterCoverage *FileStats
	for _, fs := range afterFileStats {
		if fs.Filename == targetFile {
			afterCoverage = &fs
			break
		}
	}

	if afterCoverage == nil {
		// Use before stats as fallback
		beforeCov := beforeAnalysis.Stats["coverage"].(float64)
		beforeCovered := int(beforeAnalysis.Stats["covered"].(float64))
		beforeTotal := int(beforeAnalysis.Stats["total"].(float64))
		afterCoverage = &FileStats{
			Filename: targetFile,
			Coverage: beforeCov,
			Covered:  beforeCovered,
			Total:    beforeTotal,
		}
	}

	// Calculate overall after coverage
	totalCoverage := 0.0
	for _, fs := range afterFileStats {
		totalCoverage += fs.Coverage
	}
	afterOverall := OverallCoverage{
		TotalFiles:      len(afterFileStats),
		AverageCoverage: totalCoverage / float64(len(afterFileStats)),
	}

	// Generate report
	report := generateMarkdownReport(targetFile, beforeAnalysis.Stats, *afterCoverage, beforeOverall, afterOverall, metadata)

	fmt.Println("\n================================================================================")
	fmt.Println("Coverage Improvement Report")
	fmt.Println("================================================================================")
	fmt.Println(report)

	// Save report
	reportPath := filepath.Join(outputDir, "coverage_report.md")
	if err := os.WriteFile(reportPath, []byte(report), 0644); err != nil {
		return fmt.Errorf("failed to save report: %w", err)
	}
	fmt.Printf("\nReport saved to %s\n", reportPath)

	return nil
}

// Helper functions

func runCoverage() (*CoverageData, error) {
	cmd := exec.Command("make", "coverage-summary")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to run coverage command: %w", err)
	}

	covFile := "codecov.json"
	content, err := os.ReadFile(covFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read coverage file: %w", err)
	}

	var data CoverageData
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("failed to parse coverage JSON: %w", err)
	}

	return &data, nil
}

func extractFileCoverage(data *CoverageData) []FileStats {
	var stats []FileStats

	if len(data.Data) == 0 {
		return stats
	}

	for _, fileData := range data.Data[0].Files {
		filename := fileData.Filename

		// Filter files
		if !strings.HasSuffix(filename, ".rs") {
			continue
		}
		if strings.Contains(filename, "target/") || strings.Contains(filename, ".cargo/") {
			continue
		}
		if strings.Contains(filename, "/tests/") || strings.HasSuffix(filename, "_test.rs") {
			continue
		}

		covered := fileData.Summary.Lines.Covered
		total := fileData.Summary.Lines.Count

		if total > 0 {
			coverage := (float64(covered) / float64(total)) * 100
			stats = append(stats, FileStats{
				Filename: filename,
				Coverage: coverage,
				Covered:  covered,
				Total:    total,
				Functions: map[string]int{
					"covered": fileData.Summary.Functions.Covered,
					"count":   fileData.Summary.Functions.Count,
				},
				Regions: map[string]int{
					"covered": fileData.Summary.Regions.Covered,
					"count":   fileData.Summary.Regions.Count,
				},
			})
		}
	}

	return stats
}

// collectModuleFiles collects all Rust files in the same module directory
// to provide context for test generation
func collectModuleFiles(targetFile string) (map[string]string, error) {
	moduleFiles := make(map[string]string)

	// Get the directory of the target file
	dir := filepath.Dir(targetFile)
	targetBase := filepath.Base(targetFile)

	// Read all files in the same directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Collect Rust source files (excluding the target file and test files)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		// Skip the target file itself
		if name == targetBase {
			continue
		}

		// Only include .rs files
		if !strings.HasSuffix(name, ".rs") {
			continue
		}

		// Skip test files
		if strings.HasSuffix(name, "_test.rs") || strings.Contains(name, "test") {
			continue
		}

		// Read file content
		fullPath := filepath.Join(dir, name)
		content, err := os.ReadFile(fullPath)
		if err != nil {
			// Log warning but continue
			fmt.Printf("Warning: failed to read %s: %v\n", fullPath, err)
			continue
		}

		// Add to module files with relative path for clarity
		moduleFiles[name] = string(content)
	}

	return moduleFiles, nil
}

func callGitHubModelsAPI(fileContent, filepath string, stats map[string]interface{}, moduleFiles map[string]string, modelName string) (string, error) {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return "", fmt.Errorf("GITHUB_TOKEN environment variable not set")
	}

	coverage := stats["coverage"].(float64)
	covered := int(stats["covered"].(float64))
	total := int(stats["total"].(float64))

	// Build module context section
	moduleContext := ""
	if len(moduleFiles) > 0 {
		moduleContext = "\n\nHere are other files in the same module for context:\n\n"
		for path, content := range moduleFiles {
			moduleContext += fmt.Sprintf("File: %s\n```rust\n%s\n```\n\n", path, content)
		}
	}

	prompt := fmt.Sprintf(`You are an expert Rust developer tasked with writing comprehensive unit tests.

I have a Rust source file that currently has %.2f%% test coverage (%d/%d lines covered).

Target file path: %s

Here is the target file content:

`+"```rust\n%s\n```"+`%s

Please generate comprehensive unit tests for the TARGET FILE following these requirements:

1. Focus on testing the most critical and complex functions that are currently uncovered in the TARGET FILE
2. Write tests that follow Rust best practices and conventions
3. Include tests for:
   - Normal/happy path cases
   - Edge cases and boundary conditions
   - Error handling paths
   - Different input variations
4. Use the existing test module structure if present, or create a new #[cfg(test)] module
5. Make sure tests are self-contained and don't require external dependencies when possible
6. Follow the coding style and patterns already present in the file
7. Add descriptive test names that clearly indicate what is being tested
8. Use the context from other module files to understand types, traits, and dependencies

Please provide ONLY the test code that should be added to the existing #[cfg(test)] mod tests section of the TARGET FILE, or a complete new test module if none exists. Do not include the entire file, just the test code to be added.

Format your response as:
`+"```rust\n// Your test code here\n```", coverage, covered, total, filepath, fileContent, moduleContext)

	requestBody := map[string]interface{}{
		"model": modelName,
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": "You are an expert Rust developer who writes high-quality, comprehensive unit tests.",
			},
			{
				"role":    "user",
				"content": prompt,
			},
		},
		"temperature": 0.7,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", githubModelsAPIURL, strings.NewReader(string(jsonData)))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	choices := result["choices"].([]interface{})
	if len(choices) == 0 {
		return "", fmt.Errorf("no choices in API response")
	}

	message := choices[0].(map[string]interface{})["message"].(map[string]interface{})
	content := message["content"].(string)

	// Extract code from markdown code blocks
	if strings.Contains(content, "```rust") {
		start := strings.Index(content, "```rust") + 7
		end := strings.Index(content[start:], "```")
		if end > 0 {
			content = strings.TrimSpace(content[start : start+end])
		}
	}

	return content, nil
}

func integrateTests(originalContent, generatedTests string) string {
	// Check if file already has a test module
	if strings.Contains(originalContent, "#[cfg(test)]") {
		lines := strings.Split(originalContent, "\n")

		// Find the last closing brace of the test module
		inTestModule := false
		braceCount := 0
		insertPosition := -1

		for i, line := range lines {
			if strings.Contains(line, "#[cfg(test)]") {
				inTestModule = true
				continue
			}

			if inTestModule {
				braceCount += strings.Count(line, "{")
				braceCount -= strings.Count(line, "}")

				if braceCount == 0 && strings.Contains(line, "}") {
					insertPosition = i
					break
				}
			}
		}

		if insertPosition > 0 {
			// Insert before the closing brace
			result := make([]string, 0, len(lines)+2)
			result = append(result, lines[:insertPosition]...)
			result = append(result, "")
			result = append(result, generatedTests)
			result = append(result, lines[insertPosition:]...)
			return strings.Join(result, "\n")
		}
	}

	// No existing test module, add one at the end
	return originalContent + "\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n" + generatedTests + "\n}\n"
}

func generateMarkdownReport(targetFile string, beforeStats map[string]interface{}, afterCoverage FileStats, beforeOverall, afterOverall OverallCoverage, metadata map[string]interface{}) string {
	beforeCov := beforeStats["coverage"].(float64)
	beforeCovered := int(beforeStats["covered"].(float64))
	beforeTotal := int(beforeStats["total"].(float64))

	fileImprovement := afterCoverage.Coverage - beforeCov
	linesImprovement := afterCoverage.Covered - beforeCovered
	overallImprovement := afterOverall.AverageCoverage - beforeOverall.AverageCoverage

	validationSuccess := metadata["validation_success"].(bool)
	validationAttempts := metadata["validation_attempts"].(float64)

	// Get model name from metadata, default to gpt-4.1-mini if not present
	modelName := "gpt-4.1-mini"
	if model, ok := metadata["model"].(string); ok {
		modelName = model
	}

	successIcon := "✅ Success"
	if !validationSuccess {
		successIcon = "❌ Failed"
	}

	report := fmt.Sprintf(`# Coverage Improvement Report

## Summary

This automated workflow has successfully generated and validated new unit tests to improve code coverage.

## Target File

**File:** `+"`%s`"+`

## File Coverage Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Coverage Percentage** | %.2f%% | %.2f%% | **+%.2f%%** |
| **Lines Covered** | %d/%d | %d/%d | **+%d lines** |
| **Functions Covered** | %d/%d | %d/%d | - |

## Overall Project Coverage

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| **Average Coverage** | %.2f%% | %.2f%% | **%+.2f%%** |
| **Total Files Analyzed** | %d | %d | - |

## Details

- **Validation Attempts:** %.0f
- **Validation Status:** %s
- **Test Generation Method:** GitHub Models API (%s)

## Next Steps

This PR contains automatically generated unit tests. Please review the tests to ensure they:
- Follow project coding standards
- Test meaningful scenarios
- Are maintainable and well-documented

---
*This report was automatically generated by the Coverage Improvement workflow.*
`,
		targetFile,
		beforeCov, afterCoverage.Coverage, fileImprovement,
		beforeCovered, beforeTotal, afterCoverage.Covered, afterCoverage.Total, linesImprovement,
		int(beforeStats["functions"].(map[string]interface{})["covered"].(float64)), int(beforeStats["functions"].(map[string]interface{})["count"].(float64)),
		afterCoverage.Functions["covered"], afterCoverage.Functions["count"],
		beforeOverall.AverageCoverage, afterOverall.AverageCoverage, overallImprovement,
		beforeOverall.TotalFiles, afterOverall.TotalFiles,
		validationAttempts, successIcon, modelName)

	return report
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func saveJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func loadJSON(path string, v interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}
