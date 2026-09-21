package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	hid "github.com/sstallion/go-hid"
)

const (
	vendorId  = 0x04d9
	productId = 0xa052
)

var addr string

var featureReport = [9]byte{
	0x00,
	0xc4, 0xc6, 0xc0, 0x92,
	0x40, 0x23, 0xdc, 0x96,
}

func run() error {
	if err := hid.Init(); err != nil {
		return err
	}
	defer hid.Exit()

	var (
		promHumidity = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "co2mini",
			Name:      "relative_humidity_ratio",
			Help:      "Current relative humidity as a ratio between 0 and 1.",
		})
		promTemperature = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "co2mini",
			Name:      "temperature_celsius",
			Help:      "Current temperature in degrees Celsius.",
		})
		promCarbonDioxide = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "co2mini",
			Name:      "carbon_dioxide_ppm",
			Help:      "Current carbon dioxide concentration in parts per million.",
		})
		promMeasurements = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "co2mini",
				Name:      "measurements_total",
				Help:      "Total number of measurements received from the device.",
			},
			[]string{"measurement"})
		promTimestamp = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "co2mini",
				Name:      "last_measurement_timestamp_seconds",
				Help:      "Unix timestamp of the last measurement received from the device.",
			},
			[]string{"measurement"})
	)

	promHumidity.Set(math.NaN())
	promTemperature.Set(math.NaN())
	promCarbonDioxide.Set(math.NaN())

	humidityCount := promMeasurements.WithLabelValues("relative_humidity")
	temperatureCount := promMeasurements.WithLabelValues("temperature")
	carbonDioxideCount := promMeasurements.WithLabelValues("carbon_dioxide")

	humidityTs := promTimestamp.WithLabelValues("relative_humidity")
	temperatureTs := promTimestamp.WithLabelValues("temperature")
	carbonDioxideTs := promTimestamp.WithLabelValues("carbon_dioxide")

	prometheus.MustRegister(promHumidity)
	prometheus.MustRegister(promTemperature)
	prometheus.MustRegister(promCarbonDioxide)
	prometheus.MustRegister(promMeasurements)
	prometheus.MustRegister(promTimestamp)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	srv := http.Server{
		Handler: mux,

		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	trap := make(chan os.Signal, 1)
	signal.Notify(trap, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(trap)

	var group sync.WaitGroup
	group.Go(func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			cancel(err)
			return
		}
		defer ln.Close()

		log.Printf("Listen on %s", ln.Addr())
		cause := srv.Serve(ln)
		if errors.Is(cause, http.ErrServerClosed) {
			cause = nil
		}
		cancel(cause)
	})
	group.Go(func() {
		device, err := hid.OpenFirst(vendorId, productId)
		if err != nil {
			cancel(err)
			return
		}
		defer device.Close()

		if n, err := device.SendFeatureReport(featureReport[:]); err != nil || n != len(featureReport) {
			if err == nil {
				err = errors.New("short feature report write")
			}
			cancel(err)
			return
		}

		var data [8]byte
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if n, err := device.ReadWithTimeout(data[:], 1*time.Second); err != nil || n != len(data) {
				switch {
				case errors.Is(err, hid.ErrTimeout):
					continue
				case err == nil:
					err = errors.New("short read")
				}
				cancel(err)
				return
			}
			// +-------+-------+-------+----------+------+--+--+--+
			// | item  |  hi   |  lo   | checksum | 0x0d | 0| 0| 0|
			// +-------+-------+-------+----------+------+--+--+--+
			check := data[0] + data[1] + data[2]
			if data[3] != check || data[4] != 0x0d {
				log.Printf("Malformed packet: % x", data)
				continue
			}
			value := uint16(data[1])<<8 | uint16(data[2])
			switch data[0] {
			case 0x41:
				if value == 0 {
					continue
				}
				promHumidity.Set(float64(value) / 10000)
				humidityCount.Inc()
				humidityTs.SetToCurrentTime()
			case 0x42:
				promTemperature.Set(float64(value)/16 - 273.15)
				temperatureCount.Inc()
				temperatureTs.SetToCurrentTime()
			case 0x50:
				promCarbonDioxide.Set(float64(value))
				carbonDioxideCount.Inc()
				carbonDioxideTs.SetToCurrentTime()
			}
		}
	})

	select {
	case s := <-trap:
		log.Printf("%s received", s)
		signal.Stop(trap)
		cancel(nil)
	case <-ctx.Done():
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Failed to shutdown gracefully: %v", err)
		srv.Close()
	}
	shutdownCancel()

	group.Wait()

	if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}

func main() {
	flag.StringVar(&addr, "addr", ":9200", "")
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalln(err)
	}
}
