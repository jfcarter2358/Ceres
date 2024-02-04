package constants

const ID_KEY = "_id"
const TIME_KEY = "_time"
const PREFIX_KEY = "_prefix"
const LINE_KEY = "_line"

const FILTER_AND = "$and"
const FILTER_OR = "$or"
const FILTER_NOT = "$not"
const FILTER_GT = "$gt"
const FILTER_GTE = "$gte"
const FILTER_EQ = "$eq"
const FILTER_LTE = "$lte"
const FILTER_LT = "$lt"

const GROUP_ADMIN = "admin"

const PERMISSION_ADMIN = "admin"
const PERMISSION_READ = "read"
const PERMISSION_UPDATE = "update"
const PERMISSION_WRITE = "write"

const ROLE_ADMIN = "admin"
const ROLE_READ = "read"
const ROLE_UPDATE = "update"
const ROLE_WRITE = "write"

const AUTH_DB_NAME = "_auth"
const AUTH_COLLECTION_NAME = "_users"

const DATATYPE_STRING = "string"
const DATATYPE_INT = "int"
const DATATYPE_FLOAT = "float"
const DATATYPE_BOOL = "bool"
const DATATYPE_BYTE = "byte"
const DATATYPE_ANY = "any"
const DATATYPE_LIST = "list"
const DATATYPE_DICT = "dict"

const ADJECTIVE_ASCENDING = "ASCENDING"
const ADJECTIVE_DESCENDING = "DESCENDING"

const VERB_ADD = "ADD"
const VERB_CREATE = "CREATE"
const VERB_DELETE = "DELETE"
const VERB_GET = "GET"
const VERB_INSERT = "INSERT"
const VERB_UPDATE = "UPDATE"
const VERB_ORDER = "ORDER"
const VERB_MIGRATE = "MIGRATE"
const VERB_LIMIT = "LIMIT"
const VERB_COUNT = "COUNT"
const VERB_OUTPUT = "OUTPUT"

const NOUN_COLLECTION = "COLLECTION"
const NOUN_DATABASE = "DATABASE"
const NOUN_GROUP = "GROUP"
const NOUN_PASSWORD = "PASSWORD"
const NOUN_PERMISSION = "PERMISSION"
const NOUN_RECORD = "RECORD"
const NOUN_ROLE = "ROLE"
const NOUN_SCHEMA = "SCHEMA"
const NOUN_USER = "USER"

const PREPOSITION_IN = "IN"
const PREPOSITION_INTO = "INTO"
const PREPOSITION_FROM = "FROM"
const PREPOSITION_TO = "TO"
const PREPOSITION_WHERE = "WHERE"
const PREPOSITION_WITH = "WITH"

const STATEMENT_SEP = "|"

var AUTH_SCHEMA = map[string]interface{}{
	"username": "string",
	"password": "string",
	"groups":   []interface{}{"string"},
	"roles":    []interface{}{"string"},
}

const COLD_STORAGE_PREFIX = "$coldstorage__"
