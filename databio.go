package databio

import (
	"encoding/json"
	"os"
	"path/filepath"
)

var (
	// UploadDirectory is the path to place uploaded files.
	UploadDirectory, _ = filepath.Abs("./uploads")

	// ResultDirectory is the path to place result metadata files.
	ResultDirectory, _ = filepath.Abs("./results")

	// DownloadDirectory is the path to place downloadable files.
	DownloadDirectory, _ = filepath.Abs("./downloads")

	// SourceMapsDirectory is the path to source mapping files.
	SourceMapsDirectory, _ = filepath.Abs("./sources")
)

// CheckDirectories creates necessary data directories for processing uploads.
func CheckDirectories() error {
	err := os.MkdirAll(UploadDirectory, 0777)
	if err != nil {
		return err
	}
	err = os.MkdirAll(ResultDirectory, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(DownloadDirectory, 0755)
	if err != nil {
		return err
	}
	_, err = os.Stat(SourceMapsDirectory)
	return err
}

// GetUploadPath returns the full path to an uploaded file.
func GetUploadPath(filename string) string {
	return filepath.Clean(filepath.Join(UploadDirectory, filename))
}

// GetResultPath returns the full path to a result file.
func GetResultPath(token string) string {
	return filepath.Clean(filepath.Join(ResultDirectory, token+".json"))
}

// GetDownloadPath returns the full path and URL to a downloadable file.
func GetDownloadPath(filename string) string {
	return filepath.Clean(filepath.Join(DownloadDirectory, filename))
}

// OpenSourceMap opens a source mapping file and returns it.
func OpenSourceMap(filename string) (*os.File, error) {
	return os.Open(filepath.Join(SourceMapsDirectory, filename))
}

// PutResult writes a result to the specified token.
// Either a single json-serializable data argument can be provided,
// or multiple interleaved key-value pairs.
func PutResult(token, resType string, data ...interface{}) error {
	fn := GetResultPath(token + "." + resType)
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	if len(data) == 1 {
		err = json.NewEncoder(f).Encode(data[0])
	} else {
		m := make(map[interface{}]interface{})
		for i := 0; i < len(data); i += 2 {
			m[data[i]] = data[i+1]
		}
		err = json.NewEncoder(f).Encode(m)
	}
	f.Close()
	return err
}

// GetResult reads a result from the specified token.
// Data argument should be a pointer receiver.
func GetResult(token, resType string, data interface{}) (bool, error) {
	fn := GetResultPath(token + "." + resType)
	f, err := os.Open(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return true, err
		}
		return false, err
	}
	err = json.NewDecoder(f).Decode(data)
	f.Close()
	return false, err
}
