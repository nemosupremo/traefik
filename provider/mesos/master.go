package mesos

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var errMesosNoPath = errors.New("No path specified for mesos zk lookup.")
var errMesosParseError = errors.New("Error parsing mesos master data in zk.")
var errMesosNoMaster = errors.New("Error finding mesos master.")
var errUnknownScheme = errors.New("Unknown mesos scheme.")
var errMesosUnreachable = errors.New("No reachable mesos masters.")
var errNoLabel = errors.New("No such label.")

type mesosMaster struct {
	Address struct {
		Hostname string `json:"hostname"`
		Ip       string `json:"ip"`
		Port     int    `json:"port"`
	} `json:"address"`
	Hostname string `json:"hostname"`
	ID       string `json:"id"`
	Ip       int64  `json:"ip"`
	PID      string `json:"pid"`
	Port     int    `json:"port"`
	Version  string `json:"version"`
}

type mesosTask struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	State     string `json:"state"`
	SlaveID   string `json:"slave_id"`
	SlaveIP   string `json:"-"`
	Resources struct {
		Cpus  float64 `json:"cpus"`
		Disk  float64 `json:"disk"`
		Mem   float64 `json:"mem"`
		Ports string  `json:"ports"`
	} `json:"resources"`
	Labels []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"labels"`
	Statuses []struct {
		State     string  `json:"state"`
		Timestamp float64 `json:"timestamp"`
	} `json:"statuses"`
	Container struct {
		Type   string `json:"type"`
		Docker struct {
			Image string `json:"image"`
		} `json:"docker"`
	} `json:"container"`
	Discovery struct {
		Visibility string `json:"visibility"`
		Name       string `json:"name"`
		Ports      struct {
			Ports []struct {
				Number   int    `json:"number"`
				Protocol string `json:"protocol"`
			} `json:"ports"`
		} `json:"ports"`
	}
}

type mesosState struct {
	GitSha string `json:"git_sha"`
	GitTag string `json:"git_tag"`
	Leader string `json:"leader"`
	Pid    string `json:"pid"`
	Slaves []struct {
		ID  string `json:"id"`
		PID string `json:"pid"`
	} `json:"slaves"`
	Frameworks []struct {
		ID     string       `json:"id"`
		Name   string       `json:"name"`
		Active bool         `json:"active"`
		Tasks  []*mesosTask `json:"tasks"`
	} `json:"frameworks"`
}

func (t *mesosTask) getLabelValue(name string) (string, error) {
	for _, label := range t.Labels {
		if label.Key == name {
			return label.Value, nil
		}
	}
	return "", errNoLabel
}

func (p *Provider) newRequest(method string, url string, body io.Reader) *http.Request {
	req, _ := http.NewRequest(method, url, body)
	req.Header.Add("content-type", "application/json")
	if false {
		req.Header.Add("authorization", "token=fff")
	}
	return req
}

func (p *Provider) getMesosMaster(bypassCache bool) ([]string, string, error) {
	if !bypassCache && len(p.master.Hosts) > 0 {
		if time.Since(p.master.Updated) < 30*time.Second {
			return p.master.Hosts, p.master.Protocol, nil
		}
	}

	var masterHosts []string
	protocol := "http"
	if path, err := url.Parse(p.Endpoint); err == nil {
		switch path.Scheme {
		case "zks":
			protocol = "https"
			fallthrough
		case "zk":
			if path.Path == "" || path.Path == "/" {
				return nil, protocol, errMesosNoPath
			}
			zookeeperPath := path.Path
			if zookeeperPath[0] != '/' {
				zookeeperPath = "/" + zookeeperPath
			}

			if zoo, _, err := zk.Connect(zk.FormatServers(strings.Split(path.Host, ",")), 10*time.Second); err == nil {
				defer zoo.Close()
				if children, _, err := zoo.Children(zookeeperPath); err == nil {
					sort.Strings(children)
					for _, child := range children {
						if strings.HasPrefix(child, "json.info_") {
							if data, _, err := zoo.Get(zookeeperPath + "/" + child); err == nil {
								var masterInfo mesosMaster
								if err := json.Unmarshal(data, &masterInfo); err == nil {
									masterHosts = []string{fmt.Sprintf("%s:%d", masterInfo.Address.Hostname, masterInfo.Address.Port)}
									break
								} else {
									return nil, protocol, errMesosParseError

								}
							}
						}
					}
				} else {
					return nil, protocol, errMesosNoMaster
				}
			}
		case "https":
			protocol = "https"
			fallthrough
		case "http":
			masterHosts = strings.Split(path.Host, ",")
		default:
			return nil, protocol, errUnknownScheme
		}
	} else {
		masterHosts = strings.Split(p.Endpoint, ",")
	}

	if len(masterHosts) == 0 {
		return nil, protocol, errMesosUnreachable
	}

	p.master.Hosts = masterHosts
	p.master.Protocol = protocol
	p.master.Updated = time.Now()
	return masterHosts, protocol, nil
}

func (p *Provider) getMesosState() (*mesosState, error) {
	if masterHosts, protocol, err := p.getMesosMaster(false); err == nil {
		state := new(mesosState)
		var masterErr error
		for _, host := range masterHosts {
			if resp, err := (&http.Client{}).Do(p.newRequest("GET", protocol+"://"+host+"/state", nil)); err == nil {
				err = json.NewDecoder(resp.Body).Decode(&state)
				resp.Body.Close()
				if err == nil {
					if state.Pid == state.Leader {
						masterErr = nil
						break
					}
				} else {
					masterErr = err
				}
			} else {
				masterErr = err
			}
		}
		if masterErr != nil {
			return nil, masterErr
		}
		if state.Pid != state.Leader {
			return nil, errMesosUnreachable
		}
		return state, nil
	} else {
		return nil, err
	}
}
