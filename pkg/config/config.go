package config

type NodeInfo struct {
	Addrs   string
	RootDir string
	DataDir []string
	User    string
}

type BackupConfig struct {
	Meta              string
	SpaceNames        []string
	BackendUrl        string
	MaxSSHConnections int
	User              string
	// Only for OSS for now
	MaxConcurrent int
	CommandArgs   string
	Verbose       bool
}

type RestoreConfig struct {
	Meta                string
	BackendUrl          string
	MaxSSHConnections   int
	User                string
	BackupName          string
	AllowStandaloneMeta bool
	// Only for OSS for now
	MaxConcurrent int
	CommandArgs   string
}

type CleanupConfig struct {
	BackupName string
	MetaServer []string
}

var LogPath string
