package mgconn

import (
	"context"
	"git.gdpteam.com/gogdp/gdpconfig"
	"net/url"

	"github.com/magiconair/properties"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Credentials struct {
	UserName string `gdpenv:"MDB_USER"`
	Password string `gdpenv:"MDB_PSWD"`
	Authdb   string `gdpenv:"MDB_DB"`
	Address  string `gdpenv:"MDB_ADDR"`
	Port     string `gdpenv:"MDB_PORT"`
}

func InitMongodbWithFile(f string, prefix string) (*mongo.Client, error) {

	props := properties.MustLoadFile(f, properties.UTF8)
	return InitMongodbWithProperties(props,prefix)

}

func InitWithPrefix(prefix string) (*mongo.Client, error) {
	return InitWithPropertiesPrefix("",prefix);
}

func InitWithPropertiesPrefix(fileName string, prefix string) (*mongo.Client, error) {
	config := Credentials{}
	if  fileName == "" {
		gdpconfig.LoadVariablesWithPrefix(&config,prefix)
	} else {
		gdpconfig.LoadVariablesWithPrefixAndProps(&config,prefix,fileName)
	}

	return InitMongodb(config);
}

func InitMongodbWithProperties(p *properties.Properties, prefix string) (*mongo.Client, error) {

	c := Credentials{}
	c.UserName = p.GetString(prefix+"_MDB_USER", "")
	c.Password = p.GetString(prefix+"_MDB_PSWD", "")
	c.Authdb = p.GetString(prefix+"_MDB_DB", "")
	c.Address = p.GetString(prefix+"_MDB_ADDR", "")
	c.Port = p.GetString(prefix+"_MDB_PORT", "")

	return InitMongodb(c)

}

func InitMongodb(c Credentials) (*mongo.Client, error) {

	connectionString := "mongodb://" + c.UserName + ":" + url.QueryEscape(c.Password) + "@" + c.Address + ":" + c.Port + "/?authSource=" + c.Authdb
	clientOptions := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	return client, err

}
