package benchmark

import (
	"io"
	"time"

	"turbodata"
)

// StorageProfile models a class of storage media via per-call latency and
// per-stream bandwidth. Used to wrap a real local file with synthetic remote
// behavior so a single .td/.mcap fixture can be benchmarked under multiple
// media without owning real S3/HDD hardware.
//
// Per-byte cost: bytePriceUSDPerGB and reqPriceUSD are reported alongside
// IO counters so a reader can compute total $/op without us inventing a
// synthetic dollar metric. Zero values mean "not applicable on this medium".
type StorageProfile struct {
	Name              string
	RTT               time.Duration
	PerStreamBW       int64   // bytes/sec, applied per Read/ReadAt
	ReqPriceUSD       float64 // dollars per Read/ReadAt request
	BytePriceUSDPerGB float64 // dollars per GB read
}

// Predeclared profiles. Numbers are documentary, not a contract; tune in
// one place.
var (
	// LocalNVMe: warm SSD, in-process. Latency and bandwidth dominate; no money.
	ProfileLocalNVMe = StorageProfile{
		Name:        "local_nvme",
		RTT:         20 * time.Microsecond,
		PerStreamBW: 3 << 30, // 3 GB/s
	}
	// LocalHDD: spinning disk. Seeks are expensive (modeled via RTT), bandwidth
	// limited. No money.
	ProfileLocalHDD = StorageProfile{
		Name:        "local_hdd",
		RTT:         5 * time.Millisecond,
		PerStreamBW: 150 << 20, // 150 MB/s
	}
	// CloudObjectInternal: object storage accessed from within the same cloud
	// region (e.g. EC2->S3 same-region). Bytes are free; per-request cost still
	// applies. RPS is the bottleneck.
	ProfileCloudObjInternal = StorageProfile{
		Name:        "cloud_obj_internal",
		RTT:         20 * time.Millisecond,
		PerStreamBW: 80 << 20, // 80 MB/s
		ReqPriceUSD: 0.000001,
		// BytePriceUSDPerGB: 0 (free in-region egress).
	}
	// CloudObjectPublic: object storage accessed over the public internet.
	// Bytes cost real money (egress). Per-request cost too.
	ProfileCloudObjPublic = StorageProfile{
		Name:              "cloud_obj_public",
		RTT:               20 * time.Millisecond,
		PerStreamBW:       80 << 20, // 80 MB/s
		ReqPriceUSD:       0.000001,
		BytePriceUSDPerGB: 0.09,
	}
)

// LatencyReadSource wraps a turbodata.ReadSource and injects per-call latency
// to simulate remote storage. Each Read or ReadAt sleeps for:
//
//	rtt + len(p)/perStreamBW
//
// Seek does not sleep — real object stores have no seek operation; cost is
// charged at the next Read/ReadAt as part of its RTT.
//
// Concurrency: ReadAt calls each sleep independently (no shared mutex), so N
// concurrent ReadAts complete in ~max(latency_i) wall time. This matches the
// "per-stream" bandwidth model used by ReadStrategy.StrategyForLatency.
type LatencyReadSource struct {
	rs          turbodata.ReadSource
	rtt         time.Duration
	perStreamBW int64
}

func NewLatencyReadSource(rs turbodata.ReadSource, p StorageProfile) *LatencyReadSource {
	return &LatencyReadSource{rs: rs, rtt: p.RTT, perStreamBW: p.PerStreamBW}
}

func (l *LatencyReadSource) sleep(n int) {
	if l.rtt > 0 {
		time.Sleep(l.rtt)
	}
	if l.perStreamBW > 0 && n > 0 {
		d := time.Duration(float64(n) / float64(l.perStreamBW) * float64(time.Second))
		if d > 0 {
			time.Sleep(d)
		}
	}
}

func (l *LatencyReadSource) Read(p []byte) (int, error) {
	n, err := l.rs.Read(p)
	l.sleep(n)
	return n, err
}

func (l *LatencyReadSource) Seek(offset int64, whence int) (int64, error) {
	return l.rs.Seek(offset, whence)
}

func (l *LatencyReadSource) ReadAt(p []byte, off int64) (int, error) {
	n, err := l.rs.ReadAt(p, off)
	l.sleep(n)
	return n, err
}

// Compile-time check.
var _ interface {
	io.ReadSeeker
	io.ReaderAt
} = (*LatencyReadSource)(nil)
