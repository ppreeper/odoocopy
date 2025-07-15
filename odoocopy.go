package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/fang"
	"github.com/schollz/progressbar/v3"

	"github.com/ppreeper/odoorpc"
	"github.com/ppreeper/odoorpc/odoojrpc"
	"github.com/ppreeper/odoosearchdomain"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type QueryDef struct {
	Model     string
	Filter    string
	Offset    int
	Limit     int
	Fields    string
	UpdateKey string
	NoNo      bool
}

// Conf config structure
type Host struct {
	Hostname string `default:"localhost" json:"hostname"`
	Database string `default:"odoo" json:"database"`
	Username string `default:"odoo" json:"username"`
	Password string `default:"odoo" json:"password"`
	Protocol string `default:"jsonrpc" json:"protocol,omitempty"`
	Schema   string `default:"https" json:"schema,omitempty"`
	Port     int    `default:"443" json:"port,omitempty"`
}

func NewHost() *Host {
	return &Host{
		Hostname: "localhost",
		Database: "odoo",
		Username: "odoo",
		Password: "odoo",
		Protocol: "jsonrpc",
		Schema:   "https",
		Port:     443,
	}
}

func getServerConfigByName(name string) (*Host, error) {
	if name == "" {
		return nil, fmt.Errorf("server name cannot be empty")
	}

	server := NewHost()
	server.Hostname = viper.GetString(name + ".hostname")
	server.Database = viper.GetString(name + ".database")
	server.Username = viper.GetString(name + ".username")
	server.Password = viper.GetString(name + ".password")
	protocol := viper.GetString(name + ".protocol")
	if protocol != "" {
		server.Protocol = protocol
	}
	schema := viper.GetString(name + ".schema")
	if schema != "" {
		server.Schema = schema
	}
	port := viper.GetInt(name + ".port")
	if port == 0 {
		if server.Schema == "https" {
			server.Port = 443
		} else {
			server.Port = 8069
		}
	} else {
		server.Port = port
	}

	return server, nil
}

func getOdooClient(server *Host) odoorpc.Odoo {
	odooClient := odoojrpc.NewOdoo().
		WithHostname(server.Hostname).
		WithPort(server.Port).
		WithSchema(server.Schema).
		WithDatabase(server.Database).
		WithUsername(server.Username).
		WithPassword(server.Password)
	return odooClient
}

func main() {
	// Config File
	userConfigDir, err := os.UserConfigDir()
	checkErr(err)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(userConfigDir + "/odooquery")
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Error reading config file:", err)
		return
	}

	rootCmd := &cobra.Command{
		Use:   "odoocopy <source> <destination> <model>",
		Args:  cobra.ExactArgs(3),
		Short: "Odoo Copy Tool",
		Long:  `A tool to copy data between Odoo databases.`,
		Run: func(cmd *cobra.Command, args []string) {
			source := args[0]
			destination := args[1]
			model := args[2]

			domain, _ := cmd.Flags().GetString("domain")
			offset, _ := cmd.Flags().GetInt("offset")
			limit, _ := cmd.Flags().GetInt("limit")
			fields, _ := cmd.Flags().GetString("fields")
			updateKey, _ := cmd.Flags().GetString("update-key")
			nono, _ := cmd.Flags().GetBool("nono")
			jobs, _ := cmd.Flags().GetInt("jobs")

			query := QueryDef{
				Model:     model,
				Filter:    domain,
				Offset:    offset,
				Limit:     limit,
				Fields:    fields,
				UpdateKey: updateKey,
				NoNo:      nono,
			}

			// read source flag
			sourceServer, err := getServerConfigByName(source)
			fatalErr(err, "error getting source server config")
			// fmt.Println("Source Server Config:", sourceServer)

			// read destination flag
			destinationServer, err := getServerConfigByName(destination)
			fatalErr(err, "error getting destination server config")
			// fmt.Println("Destination Server Config:", destinationServer)

			// Connect to Source DB
			sourceClient := getOdooClient(sourceServer)
			err = sourceClient.Login()
			fatalErr(err, "error logging into source server")
			// fmt.Println("connected to source DB", sourceServer.Database)

			// Connect to Destination
			destinationClient := getOdooClient(destinationServer)
			err = destinationClient.Login()
			fatalErr(err, "error logging into destination server")
			// fmt.Println("connected to destination DB:", destinationServer.Database)

			// Fetch data from Source DB
			copyRecords(sourceClient, destinationClient, jobs, model, query)

			// Transform data based on model and fields
			// Insert transformed data into Destination DB
		},
	}
	rootCmd.Flags().StringP("domain", "d", "", "Domain filter") // TODO: filter not working yet
	rootCmd.Flags().IntP("offset", "o", 0, "Offset for query")
	rootCmd.Flags().IntP("limit", "l", 0, "Limit records returned")
	rootCmd.Flags().StringP("fields", "f", "", "Comma-separated list of fields")
	rootCmd.Flags().BoolP("nono", "n", false, "No action: only show what would be done")
	rootCmd.Flags().StringP("update-key", "u", "", "Comma-separated list of fields to check for duplicate records (update if match, otherwise insert)")
	rootCmd.Flags().IntP("jobs", "j", 4, "Number of concurrent jobs (workers) to run")

	if err := fang.Execute(context.Background(), rootCmd); err != nil {
		fmt.Println("Error executing command:", err)
		os.Exit(1)
	}
}

func copyRecords(source odoorpc.Odoo, destination odoorpc.Odoo, jobs int, model string, query QueryDef) error {
	umdl := strings.ReplaceAll(model, "_", ".")

	filtp, _ := odoosearchdomain.ParseDomain(query.Filter)
	// if filtp != nil {
	// 	fmt.Println("Error searching domain:", err)
	// 	return err
	// }

	// filter with nothing in it is giving errors ,this is in the library

	fields := odoosearchdomain.Fields(query.Fields)
	updateKey := odoosearchdomain.Fields(query.UpdateKey)

	records, err := source.SearchRead(umdl, query.Offset, query.Limit, fields, filtp)
	fatalErr(err, "error fetching records from source")

	if !query.NoNo {

		progressBar := progressbar.Default(int64(len(records)), "Copying records...")

		// Create channels for fan-out/fan-in pattern
		recordChan := make(chan map[string]any, 100)
		resultChan := make(chan error, 100)

		// Number of workers
		numWorkers := jobs

		// Start workers (fan-out)
		for range numWorkers {
			go func() {
				for record := range recordChan {
					// Transform record if necessary
					destinationRecord := make(map[string]any)
					for _, field := range fields {
						if value, exists := record[field]; exists {
							destinationRecord[field] = value
						}
					}

					// Check for duplicates based on updateKey
					domain := make([]any, 0, len(updateKey))
					for _, key := range updateKey {
						if value, exists := record[key]; exists {
							domain = append(domain, []any{key, "=", value})
						}
					}
					if len(domain) > 0 {
						rid, err := destination.GetID(umdl, domain)
						if err != nil {
							resultChan <- fmt.Errorf("error searching for existing records: %v", err)
							continue
						}
						if rid > -1 {
							_, err := destination.Write(umdl, rid, destinationRecord)
							resultChan <- err
						} else {
							_, err = destination.Create(umdl, destinationRecord)
							resultChan <- err
						}
					} else {
						_, err = destination.Create(umdl, destinationRecord)
						resultChan <- err
					}
				}
			}()
		}

		// Send records to workers
		go func() {
			defer close(recordChan)
			for _, record := range records {
				recordChan <- record
			}
		}()

		// Collect results (fan-in)
		errorCount := 0
		for range records {
			err := <-resultChan
			if err != nil {
				fmt.Printf("Error creating record in destination: %v\n", err)
				errorCount++
			}
			progressBar.Add(1)
		}

		if errorCount > 0 {
			fmt.Printf("Failed to copy %d out of %d records\n", errorCount, len(records))
		}
	} else {
		jsonStr, err := json.MarshalIndent(records, "", "  ")
		fatalErr(err, "record marshalling error")
		fmt.Println(string(jsonStr))
	}

	return nil
}

func checkErr(err error, msg ...string) {
	if err != nil {
		if len(msg) == 0 {
			fmt.Printf("error: %v\n", err.Error())
		}
		fmt.Printf("%v\n", strings.Join(msg, " "))
	}
}

func fatalErr(err error, msg ...string) {
	if err != nil {
		if len(msg) == 0 {
			fmt.Printf("error: %v\n", err.Error())
		}
		fmt.Printf("%v\n", strings.Join(msg, " "))
		os.Exit(1)
	}
}
