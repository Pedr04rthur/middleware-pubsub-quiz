package metrics

import "time"

// Nanotime retorna um relógio monotônico em nanossegundos com a melhor
// resolução disponível. No Windows o relógio do Go avança em passos de ~0,5ms,
// grosso demais para as latências internas do broker; lá é usado o
// QueryPerformanceCounter. Só faz sentido comparar valores do mesmo processo.
func Nanotime() int64 {
	return nanotime()
}

// Since retorna o tempo decorrido desde start, um valor de Nanotime.
func Since(start int64) time.Duration {
	return time.Duration(Nanotime() - start)
}
