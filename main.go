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

type fileHash struct {
	FilePath string `json:"file"`
	Hash     string `json:"hash"`
}

var (
	cfg moverCfg

	pathToCFGFlag string
	UpdateDBFlag  bool

	filesHashMap map[string]string
)

func loadCfg(file string) {
	configFile, err := os.Open(file)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&cfg)
	if err != nil {
		log.Println(err)
	}
	log.Println("cfg loaded")
}

func loadDB(file string) {
	fDB, err := os.Open(cfg.PathToDB)
	if err != nil {
		log.Println(err)
	}
	defer fDB.Close()

	decoder := json.NewDecoder(fDB)
	for decoder.More() {
		if err := decoder.Decode(&filesHashMap); err != nil {
			log.Println(err)
		}
	}
	log.Println("DB loaded")
}

func saveCfg(file string) {
	fCFG, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0740)
	if err != nil {
		log.Println(err)
	}
	defer fCFG.Close()

	enc := json.NewEncoder(fCFG)
	err = enc.Encode(&cfg)
	if err != nil {
		log.Println(err)
	}
	log.Println("cfg saved")
}

func saveDB(file string, db map[string]string) {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	jsonString, err := json.Marshal(db)
	if err != nil {
		log.Println(err)
	}

	f.Write(jsonString)

	log.Println("DB saved")
}

func init() {
	flag.StringVar(&pathToCFGFlag, "pdb", "Q:\\video\\y-dl\\14\\127___03\\data\\cfg.json", "path to cfg file")
	flag.BoolVar(&UpdateDBFlag, "u", false, "re-evaluate hashes for all files")
	flag.Parse()
	// flag.Visit(func(f *flag.Flag) {
	// 	if f.Name == "u" {
	// 		fmt.Println("updatting")
	// 		// run update
	// 		return
	// 	}
	// })
	// fmt.Println("processe data")

	//load cfg
	loadCfg(pathToCFGFlag)

	// create map and load db
	filesHashMap = make(map[string]string)
	loadDB(cfg.PathToDB)
}

func getFileProducerCh(files []string) <-chan string {
	out := make(chan string, runtime.NumCPU())
	go func() {
		for _, file := range files {
			//	fmt.Println(file)
			out <- file
		}
		close(out)
	}()

	return out
}

func getHashCalculatorCh(fileIn <-chan string, totalFiles int) <-chan fileHash {
	out := make(chan fileHash, runtime.NumCPU())

	if totalFiles > runtime.NumCPU() {
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

					f.Close()

					out <- fileHash{file, fmt.Sprintf("%x", h.Sum(nil))}
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
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
}

func moveFiles() {
	countOfJobs := len(flag.Args())
	fileProducerCh := getFileProducerCh(flag.Args())
	fileNHashCh := getHashCalculatorCh(fileProducerCh, countOfJobs)

	for file := range fileNHashCh {
		if _, ok := filesHashMap[file.Hash]; ok { //exist
			log.Printf("duplicate! [%s] | [%s]", filesHashMap[file.Hash], file.FilePath)
		} else { //don't exist
			newPath := ""

			switch filepath.Ext(file.FilePath) {
			case ".jpg", ".png":
				newPath = cfg.PathToImages
			case ".flac", ".mp3":
				newPath = cfg.PathToMusic
			case ".mov", ".mp4", ".webm":
				newPath = cfg.PathToVideos
			default:
				newPath = cfg.PathToUnknown
				log.Printf("unknown extention [%s] | [%s]", filepath.Ext(filepath.Base(file.FilePath)), file.FilePath)
			}
			newPath = filepath.Join(newPath, filepath.Base(file.FilePath))

			err := moveFile(file.FilePath, newPath)
			if err != nil {
				log.Println(err)
				continue
			}

			filesHashMap[file.Hash] = newPath
			log.Printf("moved [%s]", file.FilePath)
		}
	}
}

func main() {
	if UpdateDBFlag {

		fileProducerCh := make(chan string, runtime.NumCPU())
		fileNHashCh := getHashCalculatorCh(fileProducerCh, runtime.NumCPU())

		var wg sync.WaitGroup
		wg.Add(1)
		go func(fileHashCh <-chan fileHash, wg *sync.WaitGroup) {
			// old     filesHashMap
			newFilesHashMap := make(map[string]string)
			dublicatesFilesHashMap := make(map[string]string)
			notPresentInOldFilesHashMap := make(map[string]string)

			for file := range fileHashCh {
				if _, ok := newFilesHashMap[file.Hash]; ok { //exist in new
					log.Printf("duplicate! [%s] | [%s]", filesHashMap[file.Hash], file.FilePath)
					dublicatesFilesHashMap[file.Hash] = file.FilePath
					continue
				}
				if _, ok := filesHashMap[file.Hash]; !ok { //not exist in old
					log.Printf("not present in old DB! [%s] | [%s]", filesHashMap[file.Hash], file.FilePath)
					notPresentInOldFilesHashMap[file.Hash] = file.FilePath
				}

				newFilesHashMap[file.Hash] = file.FilePath
			}

			log.Println("end total files: ", len(newFilesHashMap))
			log.Println("end duplicates: ", len(dublicatesFilesHashMap))
			for _, v := range dublicatesFilesHashMap {
				log.Println(v)
			}
			filesHashMap = newFilesHashMap
			saveDB(filepath.Join(filepath.Dir(cfg.PathToDB), time.Now().Format("2006.01.02.15.04.05")+filepath.Base(cfg.PathToDB)), filesHashMap)
			saveDB(filepath.Join(filepath.Dir(cfg.PathToDB), "dublicated"+filepath.Base(cfg.PathToDB)), dublicatesFilesHashMap)
			saveDB(filepath.Join(filepath.Dir(cfg.PathToDB), "notPresentInOldFilesHashMap"+filepath.Base(cfg.PathToDB)), notPresentInOldFilesHashMap)
			// err := os.Rename(cfg.PathToDB, newPath)
			// if err != nil {
			// 	log.Println(err)
			// }

			wg.Done()
		}(fileNHashCh, &wg)

		//PATH TO ?
		err := filepath.WalkDir(cfg.PathToVideos, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				log.Println(err)
			}
			if !d.IsDir() {
				fileProducerCh <- path
				//fmt.Println(path, d.Name(), d.IsDir())
			}
			return err
		})

		close(fileProducerCh)
		if err != nil {
			log.Println(err)
		}
		wg.Wait()
	} else {
		moveFiles()
		//save cfg
		saveCfg(cfg.PathToCFG)

		//save db
		saveDB(cfg.PathToDB, filesHashMap)
	}

	fmt.Scanln() // wait for Enter Key
}
