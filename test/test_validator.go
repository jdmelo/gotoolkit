package main

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

var defaultValidator = &StructValidator{
	models: make(map[string]*ModelRule),
}
var tagName = "valid"

var (
	minReg      = regexp.MustCompile("\b*min=[0-9]+\b*")
	maxReg      = regexp.MustCompile("\b*max=[0-9]+\b*")
	requiredReg = regexp.MustCompile("\b*required\b*")
)

const (
	T_K_NAME     = "valid"
	T_V_REQUIRED = "required"
	T_V_LEN      = "len"
	T_V_MAX      = "max"
	T_V_MIN      = "min"
	T_V_GT       = "gt" // 大于等于
	T_V_EQ       = "eq" //必须等于
	T_K_GE       = "ge" //大于等于
	T_K_LT       = "lt" //小于
	T_K_LE       = "le" //小于等于
)

// 拼接父子名称
func genFieldName(pName string, filed reflect.StructField) string {
	if filed.Anonymous {
		return pName
	} else if pName == "" {
		return filed.Name
	} else {
		return strings.Join([]string{pName, filed.Name}, ".")
	}
}

// 判断结构体filed标签中是否有valid属性,没有不需要验证
func isValidKey(field reflect.StructField) bool {
	tag := field.Tag
	_, ok := tag.Lookup(T_K_NAME)

	return ok
}

type FieldRule struct {
	Name     string       // 字段全名称: a.b.c
	Type     reflect.Type // 字段名字
	Value    string
	Required bool
	Len      int
	Min      int
	Max      int
}

func NewFiledRule(fieldName string, filed reflect.StructField) *FieldRule {
	tag := filed.Tag

	// struct filed中没有valid key不需要验证.
	if !isValidKey(filed) {
		return nil
	}

	tagV := tag.Get(tagName)
	if tagV == "-" {
		return nil
	}

	fr := &FieldRule{
		Name:  fieldName,
		Type:  filed.Type,
		Value: tagV,
	}

	// Set Required
	if requiredReg.FindString(tagV) != "" {
		fr.Required = true
	}

	// Set Min
	if l := minReg.FindAllString(tagV, -1); len(l) == 1 {
		fmt.Sscanf(l[0], "min=%d", &fr.Min)
	}

	// Set Max
	if l := maxReg.FindAllString(tagV, -1); len(l) == 1 {
		fmt.Sscanf(l[0], "max=%d", &fr.Max)
	}

	return fr
}

// 一个struct中需要校验的字段及字段校验规则缓存
type ModelRule struct {
	rules map[string]*FieldRule // key: field name, value: filed rule
}

func (m *ModelRule) CreateFieldsRule(t reflect.Type, rules map[string]*FieldRule, parentName string) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldName := genFieldName(parentName, field)

		// 递归
		if field.Type.Kind() == reflect.Struct {
			// 对象
			m.CreateFieldsRule(field.Type, rules, fieldName)
		} else if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
			// 对象指针
			m.CreateFieldsRule(field.Type.Elem(), rules, fieldName)
		} else if field.Type.Kind() == reflect.Slice {
			if field.Type.Elem().Kind() == reflect.Struct {
				// 对象数组
				m.CreateFieldsRule(field.Type.Elem(), rules, fieldName)
			} else if field.Type.Elem().Kind() == reflect.Ptr && field.Type.Elem().Elem().Kind() == reflect.Struct {
				// 对象指针数组
				m.CreateFieldsRule(field.Type.Elem().Elem(), rules, fieldName)
			}
		}

		if !isValidKey(field) {
			// 没有 valid key 不需要验证.
			continue
		}

		rule := NewFiledRule(fieldName, field)
		rules[fieldName] = rule
	}
}

// 结构体验证器
type StructValidator struct {
	rwLock sync.RWMutex
	models map[string]*ModelRule
}

func (s *StructValidator) getModelRule(modelName string) *ModelRule {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()

	return s.models[modelName]
}

func (s *StructValidator) InitModelRule(modelName string, value interface{}) *ModelRule {
	if mr := s.getModelRule(modelName); mr != nil {
		return mr
	}

	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	mr := &ModelRule{}
	rules := make(map[string]*FieldRule)
	mr.CreateFieldsRule(t, rules, "")
	mr.rules = rules

	// 加写锁
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	s.models[modelName] = mr

	return mr
}

func (s *StructValidator) Validate(modelV reflect.Value, modelT reflect.Type, rules map[string]*FieldRule, pName string) error {

	return nil
}

// 验证字段值是否合法
func (s *StructValidator) validateField(rule *FieldRule, value reflect.Value) error {

	return nil
}

func ValidateStruct(v interface{}) error {
	modelName := strings.Replace(reflect.TypeOf(v).String(), "*", "", 1)

	modelRule := defaultValidator.InitModelRule(modelName, v)

	modelValue := reflect.ValueOf(v)
	modelType := reflect.TypeOf(v)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
		modelType = modelType.Elem()
	}

	if err := defaultValidator.Validate(modelValue, modelType, modelRule.rules, ""); err != nil {
		return err
	}

	return nil
}

type TestStructTag struct {
	Name string `valid:"min=1,max=64"`
	Age  uint   `valid:"required,min=0,max=150"`
	string
}

func main() {
	tst := &TestStructTag{}

	t := reflect.TypeOf(tst)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	r := FieldRule{}

	for i := 0; i < t.NumField(); i++ {
		filed := t.Field(i)
		fmt.Println("FiledName:", filed.Name)
		fmt.Println("Anonymous:", filed.Anonymous)
	}

	fmt.Println(strings.Join([]string{"succ", ""}, "."))

	fmt.Printf("%#v.\n", r)
	fmt.Println(reflect.TypeOf(tst).String())
}
