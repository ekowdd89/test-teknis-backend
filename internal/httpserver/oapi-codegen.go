//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 --config oapi-codegen.yml ../../api/openapi-spec.yaml
package httpserver

import (
	"github.com/ekowdd89/test-teknis-backend/internal/httpserver/openapi"
)

var _ openapi.StrictServerInterface = &openapiServerImplementation{}

type openapiServerImplementation struct {
	h *HTTPServer
}

