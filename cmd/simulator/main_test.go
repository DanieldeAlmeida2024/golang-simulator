package main

import (
    "strings"
    "testing"
)

func TestServicoRecusaConfigSemMongoURI(t *testing.T) {
    t.Setenv("MONGO_URI", "")
    err := run()
    if err == nil || !strings.Contains(err.Error(), "MONGO_URI") {
        t.Fatalf("configuração sem URI deveria falhar explicitamente: %v", err)
    }
}

func TestGetEnvUsaFallbackParaVazio(t *testing.T) {
    t.Setenv("GO_SIM_TEST", "")
    if getEnv("GO_SIM_TEST", "padrao") != "padrao" {
        t.Fatal("variável vazia deve usar fallback")
    }
    t.Setenv("GO_SIM_TEST", "personalizado")
    if getEnv("GO_SIM_TEST", "padrao") != "personalizado" {
        t.Fatal("variável definida não foi aplicada")
    }
}
