package consul

import (
	"crypto/tls"
	"log"
	"net/http"
	"strconv"
	"time"

	"gitdev.inno.ktb/mfoa/share-pkg/errs.git"
	"github.com/hashicorp/consul/api"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

const (
	errConsulMsg = "get value from consul"
	ConsulCTXKey = "consuler"
)

type Config struct {
	Address            string
	Maxconns           int
	Timeout            time.Duration
	InsecureSkipVerify bool
	DefaultCacheTime   time.Duration
}

var defaultCS *Consul

func New(conf Config) *Consul {
	client, err := api.NewClient(&api.Config{
		Address: conf.Address,
		HttpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: conf.Maxconns,
				MaxConnsPerHost:     conf.Maxconns,
				TLSClientConfig:     &tls.Config{InsecureSkipVerify: conf.InsecureSkipVerify},
			},
			Timeout: conf.Timeout,
		},
	})
	if err != nil {
		log.Fatalf("cannot new consul client:%s", err)
	}

	defaultCS = &Consul{
		KV:    client.KV(),
		Cache: cache.New(conf.DefaultCacheTime, 24*time.Hour),
	}
	return defaultCS
}

type Consuler interface {
	String(key string) (string, error)
	Int(key string) (int64, error)
	Float(key string) (float64, error)
	Duration(key string) (time.Duration, error)
	GetMapErrCode(key string) (map[string]errs.Response, error)
}

type Consul struct {
	KV    *api.KV
	Cache *cache.Cache
}

func (c *Consul) String(key string) (string, error) {
	return c.get(key)
}

func (c *Consul) Int(key string) (int64, error) {
	s, err := c.get(key)
	if err != nil {
		return 0, errors.Wrap(err, errConsulMsg)
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse int from consul value, real value is %s", key)
	}

	return i, nil
}

func (c *Consul) Float(key string) (float64, error) {
	s, err := c.get(key)
	if err != nil {
		return 0, errors.Wrap(err, errConsulMsg)
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse float from consul value, real value is %s", key)
	}

	return f, nil
}

func (c *Consul) Duration(key string) (time.Duration, error) {
	s, err := c.get(key)
	if err != nil {
		return 0, errors.Wrap(err, errConsulMsg)
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, errors.Wrapf(err, "parse duration from consul value, real value is %s", key)
	}

	return d, nil
}

func (c *Consul) get(key string) (string, error) {

	i, found := c.Cache.Get(key)
	if found {
		return i.(string), nil
	}

	pair, _, err := c.KV.Get(key, nil)
	if err != nil {
		return "", err
	}

	if pair == nil {
		return "", nil
	}

	c.Cache.Set(key, string(pair.Value), cache.DefaultExpiration)

	return string(pair.Value), nil
}
