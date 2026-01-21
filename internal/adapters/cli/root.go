package cli

import (
	"fmt"
	"os"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"github.com/SHIMA0111/s3-syncer/internal/adapters/s3"
	"github.com/SHIMA0111/s3-syncer/internal/core/services"
)

var (
	srcProfile string
	dstProfile string
	srcBucket  string
	dstBucket  string
	prefix     string
	workers    int
	region     string
)

var rootCmd = &cobra.Command{
	Use:   "s3-copy",
	Short: "A high-performance S3 data migration tool",
	Long:  `s3-copy allows you to copy data between S3 buckets, extending support for cross-account copying via profiles.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		if srcBucket == "" || dstBucket == "" {
			return fmt.Errorf("source and destination buckets are required")
		}

		// Initialize adapters
		// Assuming us-east-1 default or inferred from profile, but let's allow region flag or use default.
		// For simplicity, we pass a common region or handle it in config loading.
		// We'll add a region flag.

		fmt.Printf("Initializing S3 clients...\nSrc Profile: %s\nDst Profile: %s\n", srcProfile, dstProfile)

		srcStorage, err := s3.NewAdapter(ctx, srcBucket, srcProfile, region)
		if err != nil {
			return fmt.Errorf("failed to initialize source storage: %w", err)
		}

		dstStorage, err := s3.NewAdapter(ctx, dstBucket, dstProfile, region)
		if err != nil {
			return fmt.Errorf("failed to initialize destination storage: %w", err)
		}

		// Initialize Service
		svc := services.NewCopyService(srcStorage, dstStorage, workers)

		fmt.Println("Starting migration...")

		// Progress Bar
		// We use -1 for indeterminate total initially, effectively a spinner + counter
		bar := progressbar.Default(-1, "Copying objects")

		// Callback
		onProgress := func(found, copied int64) {
			// We can update the description or just the count.
			// progressbar V3 Default(-1) is a simple spinner.
			// Ideally we want "Found: X | Copied: Y"
			// But progressbar is "X/Goal".
			// If we update Max, it might look like a completion bar.
			// queue size = found - copied.

			// Let's set Max to found.
			bar.ChangeMax64(found)
			bar.Set64(copied)
			bar.Describe(fmt.Sprintf("Found: %d | Copied", found))
		}

		err = svc.Copy(ctx, prefix, onProgress)
		if err != nil {
			return err
		}

		bar.Finish()
		fmt.Println("\nMigration completed successfully!")
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVar(&srcProfile, "src-profile", "", "AWS profile for source account")
	rootCmd.Flags().StringVar(&dstProfile, "dst-profile", "", "AWS profile for destination account")
	rootCmd.Flags().StringVar(&srcBucket, "src-bucket", "", "Source S3 bucket name")
	rootCmd.Flags().StringVar(&dstBucket, "dst-bucket", "", "Destination S3 bucket name")
	rootCmd.Flags().StringVar(&prefix, "prefix", "", "Prefix filter for keys to copy")
	rootCmd.Flags().IntVar(&workers, "workers", 10, "Number of concurrent workers")
	rootCmd.Flags().StringVar(&region, "region", "us-east-1", "AWS Region")
}
