package db

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type DBConfig struct {
	DBType        string `ini:"db_type"  validate:"nonzero"`
	IP            string `ini:"ip" validate:"nonzero"`
	Port          int    `ini:"port" validate:"nonzero"`
	User          string `ini:"user" validate:"nonzero"`
	Password      string `ini:"password" validate:"nonzero"`
	DB            string `ini:"db" validate:"nonzero"`
	Timeout       int    `ini:"timeout"`
	MaxConnection int    `ini:"max_connection"`
	MaxLifetime   int    `ini:"max_life_time"`
}

type DBClient struct {
	db *sqlx.DB
}

func CreateDBClient(cfg *DBConfig) (*DBClient, error) {
	var err error

	strConn := "%s:%s@tcp(%s:%d)/%s?autocommit=true&parseTime=true&timeout=%dms&loc=Asia%%2FShanghai&tx_isolation='READ-COMMITTED'"
	url := fmt.Sprintf(
		strConn,
		cfg.User,
		cfg.Password,
		cfg.IP,
		cfg.Port,
		cfg.DB,
		cfg.Timeout)
	db, err := sqlx.Open(cfg.DBType, url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxConnection)
	db.SetMaxIdleConns(cfg.MaxConnection)
	db.SetConnMaxLifetime(time.Second * time.Duration(cfg.MaxLifetime))

	if err = db.Ping(); err != nil {
		return nil, err
	}

	ret := &DBClient{
		db: db,
	}

	return ret, nil
}

// 向表中插入数据, 返回插入ID
func (d *DBClient) Insert(insertSql string, args ...interface{}) (int64, error) {
	stmt, err := d.db.Prepare(insertSql)
	if stmt != nil {
		defer stmt.Close()
	}
	if err != nil {
		return 0, err
	}
	ret, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}

	id, err := ret.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

// 更新表的数据, 返回更新行数.
func (d *DBClient) Update(updateSql string, args ...interface{}) (int64, error) {
	stmt, err := d.db.Prepare(updateSql)
	if stmt != nil {
		defer stmt.Close()
	}
	if err != nil {
		return 0, err
	}
	ret, err := stmt.Exec(args...)
	if err != nil {
		return 0, err
	}
	rows, err := ret.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rows, nil
}

func (d *DBClient) Query(querySql string, args ...interface{}) (int64, error) {
	stmt, err := d.db.Prepare(querySql)
	if stmt != nil {
		defer stmt.Close()
	}
	if err != nil {
		return 0, err
	}
	ret, err := stmt.Query(args...)
	if err != nil {
		return 0, err
	}
}
