package broker

import (
	"fmt"
	"github.com/go-sql-driver/mysql"
	"net/url"
	"strings"
)

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
