package telemetry

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var Tracer = otel.Tracer("kafka-lab")

func Run(ctx context.Context, service string, run func(context.Context) error) error {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return run(ctx)
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	err = run(ctx)
	const flushTimeout = 5 * time.Second
	flush, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	return errors.Join(err, provider.Shutdown(flush))
}

func End(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

type Publisher interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

func Publish(ctx context.Context, client Publisher, record *kgo.Record, eventID string) error {
	ctx, span := Tracer.Start(ctx, "publish "+record.Topic, trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("event.id", eventID)))
	otel.GetTextMapPropagator().Inject(ctx, kotel.NewRecordCarrier(record))
	err := client.ProduceSync(ctx, record).FirstErr()
	End(span, err)
	return err
}
