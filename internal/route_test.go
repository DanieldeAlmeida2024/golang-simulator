package internal

import (
    "math"
    "testing"
)

func TestFreteComArredondamentoEmCentavos(t *testing.T) {
    servico := NewFreightService()
    casos := []struct{ distancia int; esperado float64 }{
        {0, 0.30},
        {1, 0.45},
        {2, 0.60},
        {120, 18.30},
    }
    for _, caso := range casos {
        got := servico.Calculate(caso.distancia)
        if math.Abs(got-caso.esperado) > 0.000001 {
            t.Errorf("distância %d: esperado %.2f, obtido %.2f", caso.distancia, caso.esperado, got)
        }
    }
}
