package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type moverCfg struct {
	PathToDB  string `json:"db"`
	PathToCFG string `json:"cfg"`

	PathToVideos  string `json:"videos"`
	PathToMusic   string `json:"music"`
	PathToImages  string `json:"images"`
	PathToUnknown string `json:"unknown"`
}

// bad name
type fileNHash struct {
	FilePath string `json:"file"`
	Hash     string `json:"hash"`
}

var (
	cfg moverCfg

	pathToCFGFlag string
	flagUpdateDB  bool

	filesHashMap map[string]string
)

func loadCfg(file string) {
	configFile, err := os.Open(file)

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := configFile.Close(); err != nil {
			log.Println(err)
		}
	}()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&cfg); err != nil {
		log.Fatal(err)
	}
	log.Println("cfg loaded")
}

func loadDB(file string) {
	DBFile, err := os.Open(cfg.PathToDB)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := DBFile.Close(); err != nil {
			log.Println(err)
		}
	}()

	decoder := json.NewDecoder(DBFile)
	for decoder.More() {
		if err := decoder.Decode(&filesHashMap); err != nil {
			log.Println(err)
		}
	}
	log.Println("DB loaded")
}

func saveCfg(file string) {
	configFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0740)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := configFile.Close(); err != nil {
			log.Println(err)
		}
	}()

	encoder := json.NewEncoder(configFile)
	if err = encoder.Encode(&cfg); err != nil {
		log.Println(err)
	}
	log.Println("cfg saved")
}

func saveDB(file string, db map[string]string) {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	jsonString, err := json.Marshal(db)
	if err != nil {
		log.Println(err)
	}

	f.Write(jsonString)

	log.Println("DB saved")
}

func init() {
	flag.StringVar(&pathToCFGFlag, "pdb", "cfg.json", "path to cfg file")
	flag.BoolVar(&flagUpdateDB, "u", false, "re-evaluate hashes for all files")
	flag.Parse()

	//load cfg
	loadCfg(pathToCFGFlag)

	// create map and load db
	filesHashMap = make(map[string]string)
	loadDB(cfg.PathToDB)
}

func createFilePathProducerCh(files []string) <-chan string {
	out := make(chan string, runtime.NumCPU())
	go func() {
		for _, file := range files {
			out <- file
		}
		close(out)
	}()

	return out
}

func createHashCalculatorCh(fileIn <-chan string, totalFiles int) <-chan fileNHash {
	out := make(chan fileNHash, runtime.NumCPU())

	if totalFiles > runtime.NumCPU() || totalFiles <= 0 {
		totalFiles = runtime.NumCPU()
	}

	go func() {
		var wg sync.WaitGroup

		for i := 0; i < totalFiles; i++ {
			wg.Add(1)

			go func(wg *sync.WaitGroup) {
				for file := range fileIn {
					f, err := os.Open(file)
					if err != nil {
						log.Println(err)
					}

					h := sha256.New()
					if _, err := io.Copy(h, f); err != nil {
						log.Println(err)
					}

					err = f.Close()
					if err != nil {
						log.Println(err)
					}

					out <- fileNHash{file, fmt.Sprintf("%x", h.Sum(nil))}
				}
				wg.Done()
			}(&wg)
		}

		wg.Wait()
		close(out)
	}()
	return out
}

func moveFile(sourcePath, destPath string) error {
	// err := os.Rename(sourcePath, destPath)
	// if err != nil {
	// 	log.Println(err)
	// }

	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("couldn't open dest file: %s", err)
	}
	defer func() {
		if err := outputFile.Close(); err != nil {
			log.Println(err)
		}
	}()

	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	if err = os.Remove(sourcePath); err != nil {
		return fmt.Errorf("failed removing original file: %s", err)
	}
	return nil
}

func moveFiles() {
	totalFiles := len(flag.Args())
	fileProducerCh := createFilePathProducerCh(flag.Args())
	fileNHashCh := createHashCalculatorCh(fileProducerCh, totalFiles)

	for fileNHash := range fileNHashCh {
		if _, ok := filesHashMap[fileNHash.Hash]; ok { //exist
			log.Printf("duplicate! [%s] | [%s]", filesHashMap[fileNHash.Hash], fileNHash.FilePath)
		} else { //don't exist
			newPath := ""

			switch filepath.Ext(fileNHash.FilePath) {
			case ".jpg", ".png":
				newPath = cfg.PathToImages
			case ".flac", ".mp3":
				newPath = cfg.PathToMusic
			case ".mov", ".mp4", ".webm":
				newPath = cfg.PathToVideos
			default:
				newPath = cfg.PathToUnknown
				log.Printf("unknown extention [%s] | [%s]", filepath.Ext(fileNHash.FilePath), fileNHash.FilePath)
			}
			newPath = filepath.Join(newPath, filepath.Base(fileNHash.FilePath))

			if err := moveFile(fileNHash.FilePath, newPath); err != nil {
				log.Println(err)
				continue
			}

			filesHashMap[fileNHash.Hash] = newPath
			log.Printf("moved [%s]", fileNHash.FilePath)
		}
	}
}

func updateDB() {
	fileProducerCh := make(chan string, runtime.NumCPU())
	fileNHashCh := createHashCalculatorCh(fileProducerCh, runtime.NumCPU())

	var wg sync.WaitGroup
	wg.Add(1)
	go func(fileHashCh <-chan fileNHash, wg *sync.WaitGroup) {
		// old     filesHashMap
		newFilesHashMap := make(map[string]string)
		dublicates := make(map[string]string)
		notPresentInOldMap := make(map[string]string)

		for file := range fileHashCh {
			if _, ok := newFilesHashMap[file.Hash]; ok { //exist in new
				log.Printf("duplicate! [%s] | [%s]", filesHashMap[file.Hash], file.FilePath)
				dublicates[file.Hash] = file.FilePath
				continue
			}
			if _, ok := filesHashMap[file.Hash]; !ok { //not exist in old
				log.Printf("not present in old DB! [%s] | [%s]", filesHashMap[file.Hash], file.FilePath)
				notPresentInOldMap[file.Hash] = file.FilePath
			}

			newFilesHashMap[file.Hash] = file.FilePath
		}

		log.Println("end total files: ", len(newFilesHashMap))
		log.Println("duplicates: ", len(dublicates))
		for _, v := range dublicates {
			log.Println(v)
		}
		filesHashMap = newFilesHashMap
		saveDB(filepath.Join(filepath.Dir(cfg.PathToDB), time.Now().Format("2006.01.02.15.04.05")+filepath.Base(cfg.PathToDB)), filesHashMap)
		saveDB(filepath.Join(filepath.Dir(cfg.PathToDB), "dublicated"+filepath.Base(cfg.PathToDB)), dublicates)
		saveDB(filepath.Join(filepath.Dir(cfg.PathToDB), "notPresentInOldFilesHashMap"+filepath.Base(cfg.PathToDB)), notPresentInOldMap)

		wg.Done()
	}(fileNHashCh, &wg)

	//PATH TO ?
	err := filepath.WalkDir(cfg.PathToVideos, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Println(err)
		} else if !d.IsDir() {
			fileProducerCh <- path
		}
		return err
	})

	close(fileProducerCh)
	if err != nil {
		log.Println(err)
	}
	wg.Wait()

}

func main() {
	if flagUpdateDB {
		updateDB()
	} else {
		moveFiles()
		//save cfg
		saveCfg(cfg.PathToCFG)
		//save db
		saveDB(cfg.PathToDB, filesHashMap)
	}

	fmt.Scanln() // wait for Enter Key
}
