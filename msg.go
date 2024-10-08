package workers

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/bitly/go-simplejson"
	"github.com/sirupsen/logrus"
)

type data struct {
	*simplejson.Json
}

type Msg struct {
	*data
	original string
	Logger   *logrus.Entry
	Context  context.Context
}

type Args struct {
	*data
}

func (m *Msg) Jid() string {
	return m.Get("jid").MustString()
}

func (m *Msg) Args() *Args {
	if args, ok := m.CheckGet("args"); ok {
		return &Args{&data{args}}
	} else {
		d, _ := newData("[]")
		return &Args{d}
	}
}

func (m *Msg) OriginalJson() string {
	return m.original
}

func (d *data) ToJson() string {
	json, err := d.Encode()

	if err != nil {
		Logger.Println("ERR: Couldn't generate json from", d, ":", err)
	}

	return string(json)
}

func (d *data) Equals(other interface{}) bool {
	otherJson := reflect.ValueOf(other).MethodByName("ToJson").Call([]reflect.Value{})
	return d.ToJson() == otherJson[0].String()
}

func NewMsg(content string) (*Msg, error) {
	if d, err := newData(content); err != nil {
		return nil, err
	} else {
		m := &Msg{data: d, original: content, Logger: nil, Context: context.TODO()}
		m.Logger = Logger.WithField("Jid", m.Jid())
		return m, nil
	}
}

func newData(content string) (*data, error) {
	if json, err := simplejson.NewJson([]byte(content)); err != nil {
		return nil, err
	} else {
		return &data{json}, nil
	}
}

func (s *Msg) Args2Obj(obj interface{}) error {
	args := s.Args()
	data, err := args.MarshalJSON()
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, obj)
	if err != nil {
		return err
	}
	return nil
}
