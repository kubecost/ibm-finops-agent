package cldy

import "time"

const usFrontdoorURL = "https://frontdoor.apptio.com"
const euFrontdoorURL = "https://frontdoor-eu.apptio.com"
const auFrontdoorURL = "https://frontdoor-au.apptio.com"
const meFrontdoorURL = "https://frontdoor-me.apptio.com"

const usCloudabilityURL = "https://api.cloudability.com"
const euCloudabilityURL = "https://api-eu.cloudability.com"
const auCloudabilityURL = "https://api-au.cloudability.com"
const meCloudabilityURL = "https://api-me.cloudability.com"

type customerRegion int

const (
	nativeUS customerRegion = iota
	hybridEU
	hybridAU
	hybridME
	nativeEU
	nativeAU
	nativeME
)

// generic uploader, could be apptio, custom s3 or custom azure blob
type StorageService interface {
	Upload(payload UploadPayload) error
}

type UploadPayload struct {
	ClusterUID   string `json:"clusterUID"`
	FileName     string `json:"fileName"`
	AgentVersion string `json:"agentVersion"`
	UploadHash   string `json:"uploadHash"`
	FilePath     string `json:"-"`
}

type ApptioServiceImpl struct {
	keyAccess       string
	keySecret       string
	envID           string
	openToken       string
	frontdoorURL    string
	cloudabilityURL string
	validTil        time.Time
}

func NewApptioSerivce(config ApptioConfig) StorageService {
	return &ApptioServiceImpl{
		keyAccess: "",
		keySecret: "",
		envID:     "",
		openToken: "",
	}
}

type ApptioConfig struct {
	keyAccess    string
	keySecret    string
	envID        string
	openToken    string
	customerType string
}

func (s *ApptioServiceImpl) Upload(payload UploadPayload) error {
	return nil
}

// login to FD, return opentoken
func (s *ApptioServiceImpl) login() (string, error) {
	return "", nil
}

// getUploadURL request to cloudability
func (s *ApptioServiceImpl) getUploadURL() (string, error) {
	return "", nil
}
