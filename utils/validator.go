package utils

import (
	"fmt"
	"jd.com/jvirt/api-vm/common/basic"
	"jd.com/jvirt/api-vm/common/e"
	"jd.com/jvirt/api-vm/common/logger"
	"jd.com/jvirt/api-vm/common/tools"
	"jd.com/jvirt/api-vm/common/tools/json/ffjson"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

const (
	K_VALID      = "valid"     //有此标签时才对字段值进行验证，否则忽略
	K_REQUIRED   = "required"  //是否必选, 默认是false
	K_LEN        = "len"       //字符串满足指定大小
	K_MIN_LEN    = "minLen"    //字符串最小长度
	K_MAX_LEN    = "maxLen"    //字符串最大长度
	K_EQUAL      = "eq"        //必须等于
	K_BIGGER     = "gt"        //大于
	K_BIGGER_EQ  = "ge"        //大于等于
	K_LOWER      = "lt"        //小于
	K_LOWER_EQ   = "le"        //小于等于
	K_START_WITH = "startWith" //以xxx开头
	K_END_WITH   = "endWith"   //以xxx结尾
	K_REG        = "reg"       //匹配正则
	K_IN_LIST    = "inList"    //在列表中
	K_IN_LIST2   = "inList2"   //在列表中，不区分大小写
)

var (
	requiredReg = regexp.MustCompile(`\b` + K_REQUIRED + `\b`)
)

// 单例
var defaultValidator = &modelValidator{
	models: make(map[string]*propRules),
}

type Rule struct {
	Name      string       //字段全名称a.b.c
	Type      reflect.Type `json:"-"` //字段类型
	Required  bool         //是否可为空，默认为空
	Len       interface{}  `json:",omitempty"` //满足指定大小
	MinLen    interface{}  `json:",omitempty"` //最小
	MaxLen    interface{}  `json:",omitempty"` //最大
	Equal     interface{}  `json:",omitempty"` //必须等于
	Bigger    interface{}  `json:",omitempty"` //大于
	BiggerEq  interface{}  `json:",omitempty"` //大于等于
	Lower     interface{}  `json:",omitempty"` //小于
	LowerEq   interface{}  `json:",omitempty"` //小于等于
	StartWith string       `json:",omitempty"` //以xxx开头
	EndWith   string       `json:",omitempty"` //以xxx结尾
	Reg       string       `json:",omitempty"` //匹配正则
	InList    interface{}  `json:",omitempty"` //在列表中
	InList2   bool         `json:",omitempty"` //在列表中，不区分大小写
}

func (rule *Rule) setRequired(tag reflect.StructTag) {
	if requiredReg.Match([]byte(tag)) {
		rule.Required = true
	}
	rule.Required = false
}
func (rule *Rule) setLen(tag reflect.StructTag) {
	tagval := tag.Get(K_LEN)
	if tagval != "" {
		val, _ := tools.ToInt(tagval)
		rule.Len = val
	}
}
func (rule *Rule) setMinLen(tag reflect.StructTag) {
	tagval := tag.Get(K_MIN_LEN)
	if tagval != "" {
		val, _ := tools.ToInt(tagval)
		rule.MinLen = val
	}
}
func (rule *Rule) setMaxLen(tag reflect.StructTag) {
	tagval := tag.Get(K_MAX_LEN)
	if tagval != "" {
		val, _ := tools.ToInt(tagval)
		rule.MaxLen = val
	}
}
func (rule *Rule) setEqual(tag reflect.StructTag) {
	tagval := tag.Get(K_EQUAL)
	if tagval != "" {
		rule.Equal = tagval
	}
}
func (rule *Rule) setBigger(tag reflect.StructTag) {
	tagval := tag.Get(K_BIGGER)
	if tagval != "" {
		rule.Bigger = tagval
	}
}
func (rule *Rule) setBiggerEq(tag reflect.StructTag) {
	tagval := tag.Get(K_BIGGER_EQ)
	if tagval != "" {
		rule.BiggerEq = tagval
	}
}
func (rule *Rule) setLower(tag reflect.StructTag) {
	tagval := tag.Get(K_LOWER)
	if tagval != "" {
		rule.Lower = tagval
	}
}
func (rule *Rule) setLowerEq(tag reflect.StructTag) {
	tagval := tag.Get(K_LOWER_EQ)
	if tagval != "" {
		rule.LowerEq = tagval
	}
}
func (rule *Rule) setStartWith(tag reflect.StructTag) {
	tagval := tag.Get(K_START_WITH)
	if tagval != "" {
		rule.StartWith = tagval
	}
}
func (rule *Rule) setEndWith(tag reflect.StructTag) {
	tagval := tag.Get(K_END_WITH)
	if tagval != "" {
		rule.EndWith = tagval
	}
}
func (rule *Rule) setReg(tag reflect.StructTag) {
	tagval := tag.Get(K_REG)
	if tagval != "" {
		rule.Reg = tagval
	}
}
func (rule *Rule) setInList(tag reflect.StructTag) {

	// 先处理不区分大小写的，如果存在，直接返回
	tagval := tag.Get(K_IN_LIST)
	if tagval != "" {
		rule.InList = strings.Split(tagval, ",")
		return
	}

	// 如果前面不存在，则查看是否有不区分大小写的
	tagval = tag.Get(K_IN_LIST2)
	if tagval != "" {
		rule.InList = strings.Split(tagval, ",")
		rule.InList2 = true
	}
}

// 一个model中需要校验的字段，及字段校验规则缓存
type propRules struct {
	lck   sync.RWMutex
	rules map[string]*Rule //一个model中需要校验的字段，及字段校验规则缓存
}

// model缓存，保存所有需要验证的model
type modelValidator struct {
	lck    sync.RWMutex
	models map[string]*propRules //所有需要验证的model
}

// 获得model中的字段的rule
func (validator *modelValidator) initModel(modelName string, model interface{}) map[string]*Rule {

	// 获得Model类型
	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	// 规则map
	rules := make(map[string]*Rule)

	// 生成model的字段验证规则，并缓存到map中
	validator.createColsRules(modelType, rules, "")

	// 加入全局缓存
	defaultValidator.addModel(modelName, rules)

	// 返回规则map
	return rules
}

// 向model缓存中增加一个model
func (mvr *modelValidator) addModel(modelName string, rules map[string]*Rule) {

	// 将规则转换成json
	rule, _ := ffjson.Marshal(rules)

	// 打印model校验规则
	logger.Info("add validator rule -> Model=%s Rule=%s", modelName, rule)

	// 加锁
	mvr.lck.Lock()
	defer mvr.lck.Unlock()

	// 创建rule
	propRules := &propRules{
		rules: rules,
	}

	// 缓存model对应的rule
	mvr.models[modelName] = propRules
}

// 从缓存中获得一个Model的rule
func (mvr *modelValidator) getPropRules(modelName string) *propRules {
	mvr.lck.RLock()
	defer mvr.lck.RUnlock()
	return mvr.models[modelName]
}

// 获得model中的所有rule
func (mvr *modelValidator) createColsRules(model reflect.Type, rules map[string]*Rule, parentName string) {
	for i := 0; i < model.NumField(); i++ {
		field := model.Field(i)
		fieldName := getFieldName(parentName, field.Name)
		// 如果是匿名字段，字段路径中删除匿名名称
		if field.Anonymous {
			fieldName = parentName
		}
		fieldTag := field.Tag

		// 递归
		if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Struct { //对象数组
			mvr.createColsRules(field.Type.Elem(), rules, fieldName)
		} else if field.Type.Kind() == reflect.Struct { //对象
			mvr.createColsRules(field.Type, rules, fieldName)
		} else if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct { //对象指针
			mvr.createColsRules(field.Type.Elem(), rules, fieldName)
		} else if field.Type.Kind() == reflect.Slice && field.Type.Elem().Kind() == reflect.Ptr && field.Type.Elem().Elem().Kind() == reflect.Struct { //对象数组指针
			mvr.createColsRules(field.Type.Elem().Elem(), rules, fieldName)
		}

		if fieldHasValid(fieldTag) == false {
			continue
		}
		rule := &Rule{}
		rule.Name = fieldName
		rule.Type = field.Type
		rule.setRequired(fieldTag)
		rule.setLen(fieldTag)
		rule.setMinLen(fieldTag)
		rule.setMaxLen(fieldTag)
		rule.setEqual(fieldTag)
		rule.setBigger(fieldTag)
		rule.setBiggerEq(fieldTag)
		rule.setLower(fieldTag)
		rule.setLowerEq(fieldTag)
		rule.setStartWith(fieldTag)
		rule.setEndWith(fieldTag)
		rule.setReg(fieldTag)
		rule.setInList(fieldTag)
		rules[fieldName] = rule
	}
}

// 开始验证model
func (mvr *modelValidator) beginValidate(model interface{}, rules map[string]*Rule) error {
	modelValue := reflect.ValueOf(model)
	modelType := reflect.TypeOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
		modelType = modelType.Elem()
	}
	return mvr.validate(modelValue, modelType, rules, "")
}

// 验证model
func (mvr *modelValidator) validate(model reflect.Value, modelType reflect.Type, rules map[string]*Rule, pName string) error {

	for i := 0; i < model.NumField(); i++ {

		// 字段类型
		fieldType := modelType.Field(i)

		// 字段名称
		fieldName := getFieldName(pName, fieldType.Name)

		// 如果是匿名字段，字段路径中删除匿名名称
		if fieldType.Anonymous {
			fieldName = pName
		}

		// 得到字段值
		field := model.Field(i)

		// 取出指针指向的对象
		if field.Kind() == reflect.Ptr {
			// 如果指针值为空
			if field.Elem().Kind() == reflect.Invalid {
				// 此字段对应的校验规则
				rule := rules[fieldName]
				if rule == nil {
					continue
				}
				// 非空校验
				if rule.Required == false {
					return notEmptyError(fieldName)
				} else {
					continue
				}

				// 指针值不为空，取出其中的对象
			} else if field.Elem().Kind() == reflect.Struct {
				field = field.Elem()
			}
		}

		// 递归Struct
		if field.Kind() == reflect.Struct {
			// 本身非空校验
			if rule := rules[fieldName]; rule != nil {
				if rule.Required == false && field.Interface() == nil {
					return notEmptyError(fieldName)
				}
			}
			// 递归
			if err := mvr.validate(field, field.Type(), rules, fieldName); err != nil {
				return err
			}
			continue
		}

		isSlicePrt := false
		if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.Ptr {
			if field.Type().Elem().Elem().Kind() == reflect.Struct {
				isSlicePrt = true
			}
		}

		// 递归Struct数组
		if field.Kind() == reflect.Slice && (field.Type().Elem().Kind() == reflect.Struct || isSlicePrt) {

			if rule := rules[fieldName]; rule != nil {
				// 本身非空校验
				if rule.Required == false && field.Len() == 0 {
					return notEmptyError(fieldName)
				}
				// 对象数组其它规则校验
				if err := mvr.validateField(rule, field); err != nil {
					return err
				}
			}
			// 递归
			for i := 0; i < field.Len(); i++ {
				subField := field.Index(i)
				subFieldType := field.Type().Elem()
				if isSlicePrt {
					subField = subField.Elem()
					subFieldType = subFieldType.Elem()
				}
				if err := mvr.validate(subField, subFieldType, rules, fieldName); err != nil {
					return err
				}
			}
			continue
		}

		// 此字段对应的校验规则
		rule := rules[fieldName]
		if rule == nil {
			continue
		}

		// 非空校验
		if rule.Required == false && field.Interface() == nil {
			return notEmptyError(fieldName)
		}

		// 如果字段是普通类型
		// 首先进行空或非空校验
		// 如果字段可空并且value是空的，则继续循环下一个字段，如果字段不可空并且value是空的，直接返回不可空错误
		switch field.Interface().(type) {
		case string:
			if rule.Required && field.String() == "" {
				continue
			}
			if rule.Required == false && field.String() == "" {
				return notEmptyError(fieldName)
			}
		case *string:
			if rule.Required && field.IsNil() {
				continue
			}
			if rule.Required == false && field.IsNil() {
				return notEmptyError(fieldName)
			}
			field = field.Elem()
		case int:
			if rule.Required && field.Int() == 0 {
				continue
			}
		case *int:
			if rule.Required && field.IsNil() {
				continue
			}
			if rule.Required == false && field.IsNil() {
				return notEmptyError(fieldName)
			}
			field = field.Elem()
		case bool:
		case *bool:
		case float32:
			if rule.Required && field.Float() == 0 {
				continue
			}
		case float64:
			if rule.Required && field.Float() == 0 {
				continue
			}
		case []string:
			if rule.Required && field.Len() == 0 {
				continue
			}
			if rule.Required == false && field.Len() == 0 {
				return notEmptyError(fieldName)
			}
		case []int:
			if rule.Required && field.Len() == 0 {
				continue
			}
			if rule.Required == false && field.Len() == 0 {
				return notEmptyError(fieldName)
			}
		case []bool:
			if rule.Required && field.Len() == 0 {
				continue
			}
			if rule.Required == false && field.Len() == 0 {
				return notEmptyError(fieldName)
			}
		case []float32:
			if rule.Required && field.Len() == 0 {
				continue
			}
			if rule.Required == false && field.Len() == 0 {
				return notEmptyError(fieldName)
			}
		case []float64:
			if rule.Required && field.Len() == 0 {
				continue
			}
			if rule.Required == false && field.Len() == 0 {
				return notEmptyError(fieldName)
			}
		default:
			return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Validator error. Parameter %s unsupported data type", fieldName), nil)
		}

		// 到这里，value一定不是空的，对value进行校验
		if err := mvr.validateField(rule, field); err != nil {
			return err
		}
	}
	return nil
}

// 验证字段值是否合法
func (mvr *modelValidator) validateField(rule *Rule, value reflect.Value) error {
	if rule.Len != nil {
		real_len := value.Len()
		switch value.Kind() {
		case reflect.String:
			real_len = tools.Length(value.String())
		}
		if real_len != rule.Len {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.MinLen != nil {
		real_len := value.Len()
		switch value.Kind() {
		case reflect.String:
			real_len = tools.Length(value.String())
		}
		if real_len < rule.MinLen.(int) {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.MaxLen != nil {
		real_len := value.Len()
		switch value.Kind() {
		case reflect.String:
			real_len = tools.Length(value.String())
		}
		if real_len > rule.MaxLen.(int) {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.Equal != nil {
		sv := tools.ToString(value)
		if value.Kind() == reflect.Ptr {
			sv = tools.ToString(value.Elem())
		}
		if sv != rule.Equal {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.Bigger != nil {
		v, err := tools.CompareNumber(value.Interface(), rule.Bigger.(string), 1)
		if err != nil {
			return e.NewApiError(e.INVALID_ARGUMENT, err.Error(), nil)
		}
		if v == false {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.BiggerEq != nil {
		v, err := tools.CompareNumber(value.Interface(), rule.BiggerEq.(string), 2)
		if err != nil {
			return e.NewApiError(e.INVALID_ARGUMENT, err.Error(), nil)
		}
		if v == false {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.Lower != nil {
		v, err := tools.CompareNumber(value.Interface(), rule.Lower.(string), 3)
		if err != nil {
			return e.NewApiError(e.INVALID_ARGUMENT, err.Error(), nil)
		}
		if v == false {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.LowerEq != nil {
		v, err := tools.CompareNumber(value.Interface(), rule.LowerEq.(string), 4)
		if err != nil {
			return e.NewApiError(e.INVALID_ARGUMENT, err.Error(), nil)
		}
		if v == false {
			return e.NewApiError(e.OUT_OF_RANGE, fmt.Sprintf("%s out of range", rule.Name), nil)
		}
	}
	if rule.StartWith != "" {
		if ok := tools.StartWith(value.String(), rule.StartWith); ok == false {
			return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Malformed %s %s", rule.Name, value.String()), nil)
		}
	}
	if rule.EndWith != "" {
		if ok := tools.EndWith(value.String(), rule.EndWith); ok == false {
			return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Malformed %s %s", rule.Name, value.String()), nil)
		}
	}
	if rule.Reg != "" {
		if ok := tools.MatchReg(value.String(), rule.Reg); ok == false {
			return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Malformed %s %s", rule.Name, value.String()), nil)
		}
	}
	if rule.InList != nil {
		ms := rule.InList.([]string)
		switch value.Interface().(type) {
		case []string:
			for i := 0; i < value.Len(); i++ {
				if ok := tools.ContainsString2(ms, value.Index(i).String(), rule.InList2); ok == false {
					return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, value.Index(i).String()), nil)
				}
			}
		case string:
			if ok := tools.ContainsString2(ms, value.String(), rule.InList2); ok == false {
				return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, tools.ToString(value)), nil)
			}
		case []int:
			for i := 0; i < value.Len(); i++ {
				if ok := tools.ContainsString2(ms, tools.ToString(value.Index(i).Int()), rule.InList2); ok == false {
					return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, tools.ToString(value.Index(i).Int())), nil)
				}
			}
		case int:
			if ok := tools.ContainsString2(ms, tools.ToString(value.Int()), rule.InList2); ok == false {
				return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, tools.ToString(value)), nil)
			}
		case []float32:
			for i := 0; i < value.Len(); i++ {
				if ok := tools.ContainsString2(ms, tools.ToString(value.Index(i).Float()), rule.InList2); ok == false {
					return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, tools.ToString(value.Index(i).Float())), nil)
				}
			}
		case float32:
			if ok := tools.ContainsString2(ms, tools.ToString(value.Float()), rule.InList2); ok == false {
				return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, tools.ToString(value)), nil)
			}
		case []float64:
			for i := 0; i < value.Len(); i++ {
				if ok := tools.ContainsString2(ms, tools.ToString(value.Index(i).Float()), rule.InList2); ok == false {
					return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, tools.ToString(value.Index(i).Float())), nil)
				}
			}
		case float64:
			if ok := tools.ContainsString2(ms, tools.ToString(value.Float()), rule.InList2); ok == false {
				return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s'", rule.Name, tools.ToString(value)), nil)
			}
		default:
			return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Invalid %s '%s' unsupported data type", rule.Name, tools.ToString(value)), nil)
		}
	}
	return nil
}

// 非空错误
func notEmptyError(fieldName string) error {
	return e.NewApiError(e.INVALID_ARGUMENT, fmt.Sprintf("Parameter %s missing", fieldName), nil)
}

// 拼接父子名称
func getFieldName(i1 string, i2 string) string {
	if i1 == "" {
		return i2
	} else {
		return i1 + "." + i2
	}
}

// 判断结构体filed标签中是否有valid属性,没有不需要验证
func fieldHasValid(tag reflect.StructTag) bool {
	_, ok := tag.Lookup(K_VALID)

	return ok
}

// 对外接口，验证一个Model数据是否合法
func Validate(model interface{}) error {

	// model名称
	modelName := basic.GetClassName(model)

	// 获得model对应的所有字段的rule
	propRules := defaultValidator.getPropRules(modelName)

	// rules
	var rules map[string]*Rule

	// 如果缓存中没有这个model的rule，则开始初始化并加入缓存
	if propRules == nil {
		rules = defaultValidator.initModel(modelName, model)
	} else {
		rules = propRules.rules
	}

	// 开始校验
	return defaultValidator.beginValidate(model, rules)
}
