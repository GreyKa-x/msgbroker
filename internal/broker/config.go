package broker

import (
	"fmt"
	"github.com/go-sql-driver/mysql"
	"net/url"
	"strconv"
	"strings"
)

const DefaultDeliveryWorkers = 1024

func ParseDeliveryWorkers(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultDeliveryWorkers, nil
	}
	workers, err := strconv.Atoi(raw)
	if err != nil || workers < 1 {
		return 0, fmt.Errorf("DELIVERY_WORKERS must be a positive integer")
	}
	return workers, nil
}

func DatabaseDSN(dsn, raw string) (string, error) {
	if dsn != "" {
		return dsn, nil
	}
	raw = strings.Replace(raw, "mysql+pymysql://", "mysql://", 1)
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "mysql" || u.Hostname() == "" || u.User == nil || strings.Trim(u.Path, "/") == "" {
		return "", fmt.Errorf("MYSQL_DSN or MySQL DATABASE_URL required")
	}
	c := mysql.NewConfig()
	c.User = u.User.Username()
	c.Passwd, _ = u.User.Password()
	c.Net = "tcp"
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	c.Addr = u.Hostname() + ":" + port
	c.DBName = strings.TrimPrefix(u.Path, "/")
	return c.FormatDSN(), nil
}
