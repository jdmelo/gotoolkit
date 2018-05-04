package imagestore

type StoreDriver interface {
	GetDriverScheme() string
	Get()
	Add()
	Delete()
	SetAcls()
}
