package index

const STRING_TERM = "//x00"

//                db         col        var
var Indices = map[string]map[string]interface{}


func BuildIndex(database, collection string, schema interface{}) {
	if _, ok := Indices[database]; ok {
		Indices[database] = make(map[string]interface{})
	}
	idx := buildIndexAgainstSchema(schema)
	Indices[database][collection] = idx
}

// This functino assumes that the schema has already been verified
func buildIndexAgainstSchema(schema interface{}) interface{} {
	if ok := schema.(string); ok {
		typeName := schema.(string)
		switch typeName {
		case DATATYPE_STRING:
			return map[string][]string
		case DATATYPE_INT:
			return map[int][]string
		case DATATYPE_FLOAT:
			return map[float64][]string}{}
		case DATATYPE_BOOL:
			return map[bool][]string{}
		}
		return nil
	}

	if ok := schema.(map[string]interface{}); ok {
		dict := schema.(map[string]interface{})
		if len(dict) == 0 {
			return nil
		}
		out := map[string]interface{}
		for child, val := range dict {
			out[child] = buildIndexAgainstSchema(dict[child])
		}
		return out
	}

	list := schema.([]interface{})
	if ok := list[0].(string); ok {
		return buildIndexAgainstSchema(list[0])
	}
	return nil
}

func AddToIndex(id string, keys []string, schema interface{}, obj interface{}) error {
	if len(keys) > 1 {
		Indices
	}
}

func addAgainstIndex(id string, keys[string], idx, schema, obj interface{}) {
	if idx == nil {
		return
	}
	if len(keys) == 1 {
		typeName := schema.(string)
		switch typeName {
		case schema.DATATYPE_STRING:
			parts := strings.Split(obj.(string))
			if len(parts)
		case schema.DATATYPE_INT:

		case schema.DATATYPE_FLOAT:

		case schema.DATATYPE_BOOL:

		}
		// if ok := schema.(string); ok {
		// 	typeName := schema.(string)
		// 	switch typeName {
		// 	case schema.DATATYPE_STRING:
		// 		val := obj.(map[string]interface{})
		// 		totalString := fmt.Sprintf("%s%s", val, STRING_TERM)
		// 		if item, ok := obj[totalString]; ok {
		// 			temp := item.([]interface{})
		// 			obj[totalString] = append(obj[totalString], id)
		// 		} else {
		// 			obj[totalString] = []string{id}
		// 		}
		// 		parts := strings.Split(val, " ")
		// 		if len(parts) > 1 {
		// 			for _, part := range parts {
		// 				if item, ok := obj[part]; ok {
		// 					temp := item.([]interface{})
		// 					obj[part] = append(obj[part], id)
		// 				} else {
		// 					obj[part] = []string{id}
		// 				}
		// 			}
		// 		}
		// 	case schema.DATATYPE_INT:
		// 		val := obj.(map[int]interface{})
		// 		if item, ok := obj[]
		// 	case schema.DATATYPE_FLOAT:

		// 	case schema.DATATYPE_BOOL:

		// 	}
		// }
	}

}