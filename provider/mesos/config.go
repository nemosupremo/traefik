package mesos

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"text/template"

	"github.com/BurntSushi/ty/fun"
	"github.com/containous/traefik/log"
	"github.com/containous/traefik/provider"
	"github.com/containous/traefik/provider/label"
	"github.com/containous/traefik/types"
)

func (p *Provider) buildConfiguration() (*types.Configuration, []*mesosTask) {
	var mesosFuncMap = template.FuncMap{
		"getBackend":         getBackend,
		"getPort":            p.getPort,
		"getHost":            p.getHost,
		"getWeight":          getFuncApplicationStringValue(label.TraefikWeight, label.DefaultWeight),
		"getDomain":          getFuncStringValue(label.TraefikDomain, p.Domain),
		"getProtocol":        getFuncApplicationStringValue(label.TraefikProtocol, label.DefaultProtocol),
		"getPassHostHeader":  getFuncStringValue(label.TraefikFrontendPassHostHeader, label.DefaultPassHostHeader),
		"getPriority":        getFuncStringValue(label.TraefikFrontendPriority, label.DefaultFrontendPriority),
		"getEntryPoints":     getFuncSliceStringValue(label.TraefikFrontendEntryPoints),
		"getFrontendRule":    p.getFrontendRule,
		"getFrontendBackend": getFrontendBackend,
		"getID":              getID,
		"getFrontEndName":    getFrontEndName,
		"hasLabel":           hasLabel,
		"getStringValue":     getStringValue,
	}

	st, err := p.getMesosState()
	if err != nil {
		log.Errorf("Failed to create a client for Mesos, error: %v", err)
		return nil, nil
	}

	tasks := taskRecords(st)

	// filter tasks
	filteredTasks := fun.Filter(func(task *mesosTask) bool {
		return taskFilter(task, p.ExposedByDefault)
	}, tasks).([]*mesosTask)

	uniqueApps := make(map[string]*mesosTask)
	for _, value := range filteredTasks {
		if _, ok := uniqueApps[value.Discovery.Name]; !ok {
			uniqueApps[value.Discovery.Name] = value
		}
	}

	var filteredApps []*mesosTask
	for _, value := range uniqueApps {
		filteredApps = append(filteredApps, value)
	}

	templateObjects := struct {
		Applications []*mesosTask
		Tasks        []*mesosTask
		Domain       string
	}{
		Applications: filteredApps,
		Tasks:        filteredTasks,
		Domain:       p.Domain,
	}

	configuration, err := p.GetConfiguration("templates/mesos.tmpl", mesosFuncMap, templateObjects)
	if err != nil {
		log.Error(err)
	}
	return configuration, templateObjects.Tasks
}

func taskRecords(st *mesosState) []*mesosTask {
	var tasks []*mesosTask
	for _, f := range st.Frameworks {
		for _, task := range f.Tasks {
			for _, slave := range st.Slaves {
				if task.SlaveID == slave.ID {
					splits := strings.Split(slave.PID, "@")
					if len(splits) == 2 {
						if _, err := net.ResolveTCPAddr("tcp4", splits[1]); err == nil {
							task.SlaveIP, _, _ = net.SplitHostPort(splits[1])
						}
					}
				}
			}

			// only do running and discoverable tasks
			if task.State == "TASK_RUNNING" {
				tasks = append(tasks, task)
			}
		}
	}

	return tasks
}

func taskFilter(task *mesosTask, exposedByDefaultFlag bool) bool {
	if len(task.Discovery.Ports.Ports) == 0 {
		log.Debugf("Filtering Mesos task without port %s", task.Name)
		return false
	}
	if !isEnabled(task, exposedByDefaultFlag) {
		log.Debugf("Filtering disabled Mesos task %s", task.Discovery.Name)
		return false
	}

	// filter indeterminable task port
	portIndexLabel := getStringValue(task, label.TraefikPortIndex, "")
	portValueLabel := getStringValue(task, label.TraefikPort, "")
	if portIndexLabel != "" && portValueLabel != "" {
		log.Debugf("Filtering Mesos task %s specifying both %q' and %q labels", task.Name, label.TraefikPortIndex, label.TraefikPort)
		return false
	}
	if portIndexLabel != "" {
		index, err := strconv.Atoi(portIndexLabel)
		if err != nil || index < 0 || index > len(task.Discovery.Ports.Ports)-1 {
			log.Debugf("Filtering Mesos task %s with unexpected value for %q label", task.Name, label.TraefikPortIndex)
			return false
		}
	}
	if portValueLabel != "" {
		port, err := strconv.Atoi(portValueLabel)
		if err != nil {
			log.Debugf("Filtering Mesos task %s with unexpected value for %q label", task.Name, label.TraefikPort)
			return false
		}

		var foundPort bool
		for _, exposedPort := range task.Discovery.Ports.Ports {
			if port == exposedPort.Number {
				foundPort = true
				break
			}
		}

		if !foundPort {
			log.Debugf("Filtering Mesos task %s without a matching port for %q label", task.Name, label.TraefikPort)
			return false
		}
	}

	//filter healthChecks
	/*
		if task.Statuses != nil && len(task.Statuses) > 0 && task.Statuses[0].Healthy != nil && !*task.Statuses[0].Healthy {
			log.Debugf("Filtering Mesos task %s with bad healthCheck", task.Discovery.Name)
			return false

		}
	*/
	return true
}

func getID(task *mesosTask) string {
	return provider.Normalize(task.ID)
}

func getBackend(task *mesosTask, apps []*mesosTask) string {
	application, err := getApplication(task, apps)
	if err != nil {
		log.Error(err)
		return ""
	}
	return getFrontendBackend(application)
}

func getFrontendBackend(task *mesosTask) string {
	if value := getStringValue(task, label.TraefikBackend, ""); len(value) > 0 {
		return value
	}
	return "-" + provider.Normalize(task.Discovery.Name)
}

func getFrontEndName(task *mesosTask) string {
	return provider.Normalize(task.ID)
}

func (p *Provider) getPort(task *mesosTask, applications []*mesosTask) string {
	application, err := getApplication(task, applications)
	if err != nil {
		log.Error(err)
		return ""
	}

	plv := getIntValue(application, label.TraefikPortIndex, math.MinInt32, len(task.Discovery.Ports.Ports)-1)
	if plv >= 0 {
		return strconv.Itoa(task.Discovery.Ports.Ports[plv].Number)
	}

	if pv := getStringValue(application, label.TraefikPort, ""); len(pv) > 0 {
		return pv
	}

	for _, port := range task.Discovery.Ports.Ports {
		return strconv.Itoa(port.Number)
	}
	return ""
}

// getFrontendRule returns the frontend rule for the specified application, using
// it's label. It returns a default one (Host) if the label is not present.
func (p *Provider) getFrontendRule(task *mesosTask) string {
	if v := getStringValue(task, label.TraefikFrontendRule, ""); len(v) > 0 {
		return v
	}
	return "Host:" + strings.ToLower(strings.Replace(p.getSubDomain(task.Discovery.Name), "_", "-", -1)) + "." + p.Domain
}

func (p *Provider) getHost(task *mesosTask) string {
	// IPSources is always host
	return task.SlaveIP
}

func (p *Provider) getSubDomain(name string) string {
	if p.GroupsAsSubDomains {
		splitedName := strings.Split(strings.TrimPrefix(name, "/"), "/")
		provider.ReverseStringSlice(&splitedName)
		reverseName := strings.Join(splitedName, ".")
		return reverseName
	}
	return strings.Replace(strings.TrimPrefix(name, "/"), "/", "-", -1)
}

// Label functions

func hasLabel(task *mesosTask, labelName string) bool {
	for _, lbl := range task.Labels {
		if lbl.Key == labelName {
			return true
		}
	}
	return false
}

func getFuncApplicationStringValue(labelName string, defaultValue string) func(task *mesosTask, applications []*mesosTask) string {
	return func(task *mesosTask, applications []*mesosTask) string {
		app, err := getApplication(task, applications)
		if err == nil {
			return getStringValue(app, labelName, defaultValue)
		}
		log.Error(err)
		return defaultValue
	}
}

func getFuncStringValue(labelName string, defaultValue string) func(task *mesosTask) string {
	return func(task *mesosTask) string {
		return getStringValue(task, labelName, defaultValue)
	}
}

func getFuncSliceStringValue(labelName string) func(task *mesosTask) []string {
	return func(task *mesosTask) []string {
		return getSliceStringValue(task, labelName)
	}
}

func getStringValue(task *mesosTask, labelName string, defaultValue string) string {
	for _, lbl := range task.Labels {
		if lbl.Key == labelName {
			return lbl.Value
		}
	}
	return defaultValue
}

func getBoolValue(task *mesosTask, labelName string, defaultValue bool) bool {
	for _, lbl := range task.Labels {
		if lbl.Key == labelName {
			v, err := strconv.ParseBool(lbl.Value)
			if err == nil {
				return v
			}
		}
	}
	return defaultValue
}

func getIntValue(task *mesosTask, labelName string, defaultValue int, maxValue int) int {
	for _, lbl := range task.Labels {
		if lbl.Key == labelName {
			value, err := strconv.Atoi(lbl.Value)
			if err == nil {
				if value <= maxValue {
					return value
				}
				log.Warnf("The value %q for %q exceed the max authorized value %q, falling back to %v.", lbl.Value, labelName, maxValue, defaultValue)
			} else {
				log.Warnf("Unable to parse %q: %q, falling back to %v. %v", labelName, lbl.Value, defaultValue, err)
			}
		}
	}
	return defaultValue
}

func getSliceStringValue(task *mesosTask, labelName string) []string {
	for _, lbl := range task.Labels {
		if lbl.Key == labelName {
			return label.SplitAndTrimString(lbl.Value, ",")
		}
	}
	return nil
}

func getApplication(task *mesosTask, apps []*mesosTask) (*mesosTask, error) {
	for _, app := range apps {
		if app.Discovery.Name == task.Discovery.Name {
			return app, nil
		}
	}
	return nil, fmt.Errorf("unable to get Mesos application from task %s", task.Discovery.Name)
}

func isEnabled(task *mesosTask, exposedByDefault bool) bool {
	return getBoolValue(task, label.TraefikEnable, exposedByDefault)
}
