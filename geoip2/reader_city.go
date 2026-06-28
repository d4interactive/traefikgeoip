package geoip2

import (
	"errors"
	"io/ioutil"
	"net"
	"strconv"
	"sync"
)

// cityReaderCache shares a single CityReader (and its ~66MB DB buffer) per file path
// across all middleware instances. Traefik builds a new plugin instance per router that
// references the middleware; without this cache each instance would ReadFile the whole
// DB into heap, multiplying memory by the number of routes and OOM-killing Traefik.
// The MaxMind reader is read-only and safe for concurrent use.
var (
	cityReaderCache   = map[string]*CityReader{}
	cityReaderCacheMu sync.Mutex
)

type CityReader struct {
	*reader
}

func (r *CityReader) Lookup(ip net.IP) (*CityResult, error) {
	offset, err := r.getOffset(ip)
	if err != nil {
		return nil, err
	}
	dataType, size, offset, err := readControl(r.decoderBuffer, offset)
	if err != nil {
		return nil, err
	}
	if dataType != dataTypeMap {
		return nil, errors.New("invalid City type: " + strconv.Itoa(int(dataType)))
	}
	var key []byte
	result := &CityResult{}
	for i := uint(0); i < size; i++ {
		key, offset, err = readMapKey(r.decoderBuffer, offset)
		if err != nil {
			return nil, err
		}
		switch bytesToKeyString(key) {
		case "city":
			offset, err = readCity(&result.City, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "continent":
			offset, err = readContinent(&result.Continent, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "country":
			offset, err = readCountry(&result.Country, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "location":
			offset, err = readLocation(&result.Location, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "postal":
			offset, err = readPostal(&result.Postal, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "registered_country":
			offset, err = readCountry(&result.RegisteredCountry, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "represented_country":
			offset, err = readCountry(&result.RepresentedCountry, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "subdivisions":
			result.Subdivisions, offset, err = readSubdivisions(r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		case "traits":
			offset, err = readTraits(&result.Traits, r.decoderBuffer, offset)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("unknown City response key: " + string(key) + ", type: " + strconv.Itoa(int(dataType)))
		}
	}
	return result, nil
}

func NewCityReader(buffer []byte) (*CityReader, error) {
	reader, err := newReader(buffer)
	if err != nil {
		return nil, err
	}
	if reader.metadata.DatabaseType != "GeoIP2-City" &&
		reader.metadata.DatabaseType != "GeoLite2-City" &&
		reader.metadata.DatabaseType != "GeoIP2-Enterprise" &&
		reader.metadata.DatabaseType != "DBIP-City-Lite" {
		return nil, errors.New("wrong MaxMind DB City type: " + reader.metadata.DatabaseType)
	}
	return &CityReader{
		reader: reader,
	}, nil
}

func NewCityReaderFromFile(filename string) (*CityReader, error) {
	cityReaderCacheMu.Lock()
	defer cityReaderCacheMu.Unlock()
	if r, ok := cityReaderCache[filename]; ok {
		return r, nil
	}
	buffer, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	r, err := NewCityReader(buffer)
	if err != nil {
		return nil, err
	}
	cityReaderCache[filename] = r
	return r, nil
}

func NewEnterpriseReader(buffer []byte) (*CityReader, error) {
	return NewCityReader(buffer)
}

func NewEnterpriseReaderFromFile(filename string) (*CityReader, error) {
	return NewCityReaderFromFile(filename)
}
