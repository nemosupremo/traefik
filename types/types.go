package types

import (
	"crypto/tls"
	"crypto/x509"
	"encoding"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/containous/flaeg"
	"github.com/containous/traefik/log"
	traefikTls "github.com/containous/traefik/tls"
	"github.com/docker/libkv/store"
	"github.com/ryanuber/go-glob"
)

// Backend holds backend configuration.
type Backend struct {
	Servers        map[string]Server `json:"servers,omitempty"`
	CircuitBreaker *CircuitBreaker   `json:"circuitBreaker,omitempty"`
	LoadBalancer   *LoadBalancer     `json:"loadBalancer,omitempty"`
	MaxConn        *MaxConn          `json:"maxConn,omitempty"`
	HealthCheck    *HealthCheck      `json:"healthCheck,omitempty"`
}

// MaxConn holds maximum connection configuration
type MaxConn struct {
	Amount        int64  `json:"amount,omitempty"`
	ExtractorFunc string `json:"extractorFunc,omitempty"`
}

// LoadBalancer holds load balancing configuration.
type LoadBalancer struct {
	Method     string      `json:"method,omitempty"`
	Sticky     bool        `json:"sticky,omitempty"` // Deprecated: use Stickiness instead
	Stickiness *Stickiness `json:"stickiness,omitempty"`
}

// Stickiness holds sticky session configuration.
type Stickiness struct {
	CookieName string `json:"cookieName,omitempty"`
}

// CircuitBreaker holds circuit breaker configuration.
type CircuitBreaker struct {
	Expression string `json:"expression,omitempty"`
}

// HealthCheck holds HealthCheck configuration
type HealthCheck struct {
	Path     string `json:"path,omitempty"`
	Port     int    `json:"port,omitempty"`
	Interval string `json:"interval,omitempty"`
}

// Server holds server configuration.
type Server struct {
	URL    string `json:"url,omitempty"`
	Weight int    `json:"weight"`
}

// Route holds route configuration.
type Route struct {
	Rule string `json:"rule,omitempty"`
}

//ErrorPage holds custom error page configuration
type ErrorPage struct {
	Status  []string `json:"status,omitempty"`
	Backend string   `json:"backend,omitempty"`
	Query   string   `json:"query,omitempty"`
}

// Rate holds a rate limiting configuration for a specific time period
type Rate struct {
	Period  flaeg.Duration `json:"period,omitempty"`
	Average int64          `json:"average,omitempty"`
	Burst   int64          `json:"burst,omitempty"`
}

// RateLimit holds a rate limiting configuration for a given frontend
type RateLimit struct {
	RateSet       map[string]*Rate `json:"rateset,omitempty"`
	ExtractorFunc string           `json:"extractorFunc,omitempty"`
}

// Headers holds the custom header configuration
type Headers struct {
	CustomRequestHeaders    map[string]string `json:"customRequestHeaders,omitempty"`
	CustomResponseHeaders   map[string]string `json:"customResponseHeaders,omitempty"`
	AllowedHosts            []string          `json:"allowedHosts,omitempty"`
	HostsProxyHeaders       []string          `json:"hostsProxyHeaders,omitempty"`
	SSLRedirect             bool              `json:"sslRedirect,omitempty"`
	SSLTemporaryRedirect    bool              `json:"sslTemporaryRedirect,omitempty"`
	SSLHost                 string            `json:"sslHost,omitempty"`
	SSLProxyHeaders         map[string]string `json:"sslProxyHeaders,omitempty"`
	STSSeconds              int64             `json:"stsSeconds,omitempty"`
	STSIncludeSubdomains    bool              `json:"stsIncludeSubdomains,omitempty"`
	STSPreload              bool              `json:"stsPreload,omitempty"`
	ForceSTSHeader          bool              `json:"forceSTSHeader,omitempty"`
	FrameDeny               bool              `json:"frameDeny,omitempty"`
	CustomFrameOptionsValue string            `json:"customFrameOptionsValue,omitempty"`
	ContentTypeNosniff      bool              `json:"contentTypeNosniff,omitempty"`
	BrowserXSSFilter        bool              `json:"browserXssFilter,omitempty"`
	ContentSecurityPolicy   string            `json:"contentSecurityPolicy,omitempty"`
	PublicKey               string            `json:"publicKey,omitempty"`
	ReferrerPolicy          string            `json:"referrerPolicy,omitempty"`
	IsDevelopment           bool              `json:"isDevelopment,omitempty"`
}

// HasCustomHeadersDefined checks to see if any of the custom header elements have been set
func (h Headers) HasCustomHeadersDefined() bool {
	return len(h.CustomResponseHeaders) != 0 ||
		len(h.CustomRequestHeaders) != 0
}

// HasSecureHeadersDefined checks to see if any of the secure header elements have been set
func (h Headers) HasSecureHeadersDefined() bool {
	return len(h.AllowedHosts) != 0 ||
		len(h.HostsProxyHeaders) != 0 ||
		h.SSLRedirect ||
		h.SSLTemporaryRedirect ||
		h.SSLHost != "" ||
		len(h.SSLProxyHeaders) != 0 ||
		h.STSSeconds != 0 ||
		h.STSIncludeSubdomains ||
		h.STSPreload ||
		h.ForceSTSHeader ||
		h.FrameDeny ||
		h.CustomFrameOptionsValue != "" ||
		h.ContentTypeNosniff ||
		h.BrowserXSSFilter ||
		h.ContentSecurityPolicy != "" ||
		h.PublicKey != "" ||
		h.ReferrerPolicy != "" ||
		h.IsDevelopment
}

// Frontend holds frontend configuration.
type Frontend struct {
	EntryPoints              []string             `json:"entryPoints,omitempty"`
	Backend                  string               `json:"backend,omitempty"`
	Routes                   map[string]Route     `json:"routes,omitempty"`
	PassHostHeader           bool                 `json:"passHostHeader,omitempty"`
	PassTLSCert              bool                 `json:"passTLSCert,omitempty"`
	Priority                 int                  `json:"priority"`
	BasicAuth                []string             `json:"basicAuth"`
	WhitelistSourceRange     []string             `json:"whitelistSourceRange,omitempty"`
	WhitelistTrustProxy      []string             `json:"whitelistTrustProxy,omitempty"`
	AuthWhitelistSourceRange []string             `json:"authWhitelistSourceRange,omitempty"`
	AuthWhitelistTrustProxy  []string             `json:"authWhitelistTrustProxy,omitempty"`
	Headers                  Headers              `json:"headers,omitempty"`
	Errors                   map[string]ErrorPage `json:"errors,omitempty"`
	RateLimit                *RateLimit           `json:"ratelimit,omitempty"`
	Redirect                 string               `json:"redirect,omitempty"`
}

// LoadBalancerMethod holds the method of load balancing to use.
type LoadBalancerMethod uint8

const (
	// Wrr (default) = Weighted Round Robin
	Wrr LoadBalancerMethod = iota
	// Drr = Dynamic Round Robin
	Drr
)

var loadBalancerMethodNames = []string{
	"Wrr",
	"Drr",
}

// NewLoadBalancerMethod create a new LoadBalancerMethod from a given LoadBalancer.
func NewLoadBalancerMethod(loadBalancer *LoadBalancer) (LoadBalancerMethod, error) {
	var method string
	if loadBalancer != nil {
		method = loadBalancer.Method
		for i, name := range loadBalancerMethodNames {
			if strings.EqualFold(name, method) {
				return LoadBalancerMethod(i), nil
			}
		}
	}
	return Wrr, fmt.Errorf("invalid load-balancing method '%s'", method)
}

// Configurations is for currentConfigurations Map
type Configurations map[string]*Configuration

// Configuration of a provider.
type Configuration struct {
	Backends         map[string]*Backend         `json:"backends,omitempty"`
	Frontends        map[string]*Frontend        `json:"frontends,omitempty"`
	TLSConfiguration []*traefikTls.Configuration `json:"tlsConfiguration,omitempty"`
}

// ConfigMessage hold configuration information exchanged between parts of traefik.
type ConfigMessage struct {
	ProviderName  string
	Configuration *Configuration
}

// Constraint hold a parsed constraint expression
type Constraint struct {
	Key string `export:"true"`
	// MustMatch is true if operator is "==" or false if operator is "!="
	MustMatch bool `export:"true"`
	// TODO: support regex
	Regex string `export:"true"`
}

// NewConstraint receive a string and return a *Constraint, after checking syntax and parsing the constraint expression
func NewConstraint(exp string) (*Constraint, error) {
	sep := ""
	constraint := &Constraint{}

	if strings.Contains(exp, "==") {
		sep = "=="
		constraint.MustMatch = true
	} else if strings.Contains(exp, "!=") {
		sep = "!="
		constraint.MustMatch = false
	} else {
		return nil, errors.New("constraint expression missing valid operator: '==' or '!='")
	}

	kv := strings.SplitN(exp, sep, 2)
	if len(kv) == 2 {
		// At the moment, it only supports tags
		if kv[0] != "tag" {
			return nil, errors.New("constraint must be tag-based. Syntax: tag==us-*")
		}

		constraint.Key = kv[0]
		constraint.Regex = kv[1]
		return constraint, nil
	}

	return nil, fmt.Errorf("incorrect constraint expression: %s", exp)
}

func (c *Constraint) String() string {
	if c.MustMatch {
		return c.Key + "==" + c.Regex
	}
	return c.Key + "!=" + c.Regex
}

var _ encoding.TextUnmarshaler = (*Constraint)(nil)

// UnmarshalText define how unmarshal in TOML parsing
func (c *Constraint) UnmarshalText(text []byte) error {
	constraint, err := NewConstraint(string(text))
	if err != nil {
		return err
	}
	c.Key = constraint.Key
	c.MustMatch = constraint.MustMatch
	c.Regex = constraint.Regex
	return nil
}

var _ encoding.TextMarshaler = (*Constraint)(nil)

// MarshalText encodes the receiver into UTF-8-encoded text and returns the result.
func (c *Constraint) MarshalText() (text []byte, err error) {
	return []byte(c.String()), nil
}

// MatchConstraintWithAtLeastOneTag tests a constraint for one single service
func (c *Constraint) MatchConstraintWithAtLeastOneTag(tags []string) bool {
	for _, tag := range tags {
		if glob.Glob(c.Regex, tag) {
			return true
		}
	}
	return false
}

//Set []*Constraint
func (cs *Constraints) Set(str string) error {
	exps := strings.Split(str, ",")
	if len(exps) == 0 {
		return fmt.Errorf("bad Constraint format: %s", str)
	}
	for _, exp := range exps {
		constraint, err := NewConstraint(exp)
		if err != nil {
			return err
		}
		*cs = append(*cs, constraint)
	}
	return nil
}

// Constraints holds a Constraint parser
type Constraints []*Constraint

//Get []*Constraint
func (cs *Constraints) Get() interface{} { return []*Constraint(*cs) }

//String returns []*Constraint in string
func (cs *Constraints) String() string { return fmt.Sprintf("%+v", *cs) }

//SetValue sets []*Constraint into the parser
func (cs *Constraints) SetValue(val interface{}) {
	*cs = Constraints(val.(Constraints))
}

// Type exports the Constraints type as a string
func (cs *Constraints) Type() string {
	return "constraint"
}

// Store holds KV store cluster config
type Store struct {
	store.Store
	// like this "prefix" (without the /)
	Prefix string `export:"true"`
}

// Cluster holds cluster config
type Cluster struct {
	Node  string `description:"Node name" export:"true"`
	Store *Store `export:"true"`
}

// Auth holds authentication configuration (BASIC, DIGEST, users)
type Auth struct {
	Basic                *Basic   `export:"true"`
	Digest               *Digest  `export:"true"`
	Forward              *Forward `export:"true"`
	WhitelistSourceRange []string `export:"true" mapstructure:","`
	WhitelistTrustProxy  []string `export:"true" mapstructure:","`
	HeaderField          string   `export:"true"`
}

// Users authentication users
type Users []string

// Basic HTTP basic authentication
type Basic struct {
	Users     `mapstructure:","`
	UsersFile string
}

// Digest HTTP authentication
type Digest struct {
	Users     `mapstructure:","`
	UsersFile string
}

// Forward authentication
type Forward struct {
	Address            string     `description:"Authentication server address"`
	TLS                *ClientTLS `description:"Enable TLS support" export:"true"`
	TrustForwardHeader bool       `description:"Trust X-Forwarded-* headers" export:"true"`
}

// CanonicalDomain returns a lower case domain with trim space
func CanonicalDomain(domain string) string {
	return strings.ToLower(strings.TrimSpace(domain))
}

// Statistics provides options for monitoring request and response stats
type Statistics struct {
	RecentErrors int `description:"Number of recent errors logged" export:"true"`
}

// Metrics provides options to expose and send Traefik metrics to different third party monitoring systems
type Metrics struct {
	Prometheus *Prometheus `description:"Prometheus metrics exporter type" export:"true"`
	Datadog    *Datadog    `description:"DataDog metrics exporter type" export:"true"`
	StatsD     *Statsd     `description:"StatsD metrics exporter type" export:"true"`
	InfluxDB   *InfluxDB   `description:"InfluxDB metrics exporter type"`
}

// Prometheus can contain specific configuration used by the Prometheus Metrics exporter
type Prometheus struct {
	Buckets    Buckets `description:"Buckets for latency metrics" export:"true"`
	EntryPoint string  `description:"EntryPoint" export:"true"`
}

// Datadog contains address and metrics pushing interval configuration
type Datadog struct {
	Address      string `description:"DataDog's address"`
	PushInterval string `description:"DataDog push interval" export:"true"`
}

// Statsd contains address and metrics pushing interval configuration
type Statsd struct {
	Address      string `description:"StatsD address"`
	PushInterval string `description:"StatsD push interval" export:"true"`
}

// InfluxDB contains address and metrics pushing interval configuration
type InfluxDB struct {
	Address      string `description:"InfluxDB address"`
	PushInterval string `description:"InfluxDB push interval"`
}

// Buckets holds Prometheus Buckets
type Buckets []float64

//Set adds strings elem into the the parser
//it splits str on "," and ";" and apply ParseFloat to string
func (b *Buckets) Set(str string) error {
	fargs := func(c rune) bool {
		return c == ',' || c == ';'
	}
	// get function
	slice := strings.FieldsFunc(str, fargs)
	for _, bucket := range slice {
		bu, err := strconv.ParseFloat(bucket, 64)
		if err != nil {
			return err
		}
		*b = append(*b, bu)
	}
	return nil
}

//Get []float64
func (b *Buckets) Get() interface{} { return Buckets(*b) }

//String return slice in a string
func (b *Buckets) String() string { return fmt.Sprintf("%v", *b) }

//SetValue sets []float64 into the parser
func (b *Buckets) SetValue(val interface{}) {
	*b = Buckets(val.(Buckets))
}

// TraefikLog holds the configuration settings for the traefik logger.
type TraefikLog struct {
	FilePath string `json:"file,omitempty" description:"Traefik log file path. Stdout is used when omitted or empty"`
	Format   string `json:"format,omitempty" description:"Traefik log format: json | common"`
}

// AccessLog holds the configuration settings for the access logger (middlewares/accesslog).
type AccessLog struct {
	FilePath string `json:"file,omitempty" description:"Access log file path. Stdout is used when omitted or empty" export:"true"`
	Format   string `json:"format,omitempty" description:"Access log format: json | common" export:"true"`
}

// ClientTLS holds TLS specific configurations as client
// CA, Cert and Key can be either path or file contents
type ClientTLS struct {
	CA                 string `description:"TLS CA"`
	CAOptional         bool   `description:"TLS CA.Optional"`
	Cert               string `description:"TLS cert"`
	Key                string `description:"TLS key"`
	InsecureSkipVerify bool   `description:"TLS insecure skip verify"`
}

// CreateTLSConfig creates a TLS config from ClientTLS structures
func (clientTLS *ClientTLS) CreateTLSConfig() (*tls.Config, error) {
	var err error
	if clientTLS == nil {
		log.Warnf("clientTLS is nil")
		return nil, nil
	}
	caPool := x509.NewCertPool()
	clientAuth := tls.NoClientCert
	if clientTLS.CA != "" {
		var ca []byte
		if _, errCA := os.Stat(clientTLS.CA); errCA == nil {
			ca, err = ioutil.ReadFile(clientTLS.CA)
			if err != nil {
				return nil, fmt.Errorf("Failed to read CA. %s", err)
			}
		} else {
			ca = []byte(clientTLS.CA)
		}
		caPool.AppendCertsFromPEM(ca)
		if clientTLS.CAOptional {
			clientAuth = tls.VerifyClientCertIfGiven
		} else {
			clientAuth = tls.RequireAndVerifyClientCert
		}
	}

	cert := tls.Certificate{}
	_, errKeyIsFile := os.Stat(clientTLS.Key)

	if !clientTLS.InsecureSkipVerify && (len(clientTLS.Cert) == 0 || len(clientTLS.Key) == 0) {
		return nil, fmt.Errorf("TLS Certificate or Key file must be set when TLS configuration is created")
	}

	if len(clientTLS.Cert) > 0 && len(clientTLS.Key) > 0 {
		if _, errCertIsFile := os.Stat(clientTLS.Cert); errCertIsFile == nil {
			if errKeyIsFile == nil {
				cert, err = tls.LoadX509KeyPair(clientTLS.Cert, clientTLS.Key)
				if err != nil {
					return nil, fmt.Errorf("Failed to load TLS keypair: %v", err)
				}
			} else {
				return nil, fmt.Errorf("tls cert is a file, but tls key is not")
			}
		} else {
			if errKeyIsFile != nil {
				cert, err = tls.X509KeyPair([]byte(clientTLS.Cert), []byte(clientTLS.Key))
				if err != nil {
					return nil, fmt.Errorf("Failed to load TLS keypair: %v", err)

				}
			} else {
				return nil, fmt.Errorf("tls key is a file, but tls cert is not")
			}
		}
	}

	TLSConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caPool,
		InsecureSkipVerify: clientTLS.InsecureSkipVerify,
		ClientAuth:         clientAuth,
	}
	return TLSConfig, nil
}
