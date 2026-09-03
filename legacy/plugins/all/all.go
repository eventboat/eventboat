package all

import (
	_ "github.com/riverpod/riverpod/plugins/codec/json"
	_ "github.com/riverpod/riverpod/plugins/codec/raw"
	_ "github.com/riverpod/riverpod/plugins/sink/drop"
	_ "github.com/riverpod/riverpod/plugins/sink/http"
	_ "github.com/riverpod/riverpod/plugins/sink/kafka"
	_ "github.com/riverpod/riverpod/plugins/source/cron"
	_ "github.com/riverpod/riverpod/plugins/source/httpserver"
	_ "github.com/riverpod/riverpod/plugins/source/kafka"
	_ "github.com/riverpod/riverpod/plugins/transform/filter"
	_ "github.com/riverpod/riverpod/plugins/transform/map"
	_ "github.com/riverpod/riverpod/plugins/transform/route"
	_ "github.com/riverpod/riverpod/plugins/transform/wasm"
)
