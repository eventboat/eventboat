package all

import (
	_ "github.com/edgesets/edgestream/plugins/codec/json"
	_ "github.com/edgesets/edgestream/plugins/codec/raw"
	_ "github.com/edgesets/edgestream/plugins/sink/drop"
	_ "github.com/edgesets/edgestream/plugins/sink/http"
	_ "github.com/edgesets/edgestream/plugins/sink/kafka"
	_ "github.com/edgesets/edgestream/plugins/source/cron"
	_ "github.com/edgesets/edgestream/plugins/source/httpserver"
	_ "github.com/edgesets/edgestream/plugins/source/kafka"
	_ "github.com/edgesets/edgestream/plugins/transform/filter"
	_ "github.com/edgesets/edgestream/plugins/transform/map"
	_ "github.com/edgesets/edgestream/plugins/transform/route"
	_ "github.com/edgesets/edgestream/plugins/transform/wasm"
)
