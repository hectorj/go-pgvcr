package pgvcr

import (
	"context"
	"log/slog"
)

type NoopHandler struct{}

func (n NoopHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return false
}

func (n NoopHandler) Handle(_ context.Context, _ slog.Record) error {
	return nil
}

func (n NoopHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return n
}

func (n NoopHandler) WithGroup(_ string) slog.Handler {
	return n
}

type contextKeySloggerType string

const contextKeySlogger contextKeySloggerType = "slogger"

func loggerFromContext(ctx context.Context) *slog.Logger {
	l, ok := ctx.Value(contextKeySlogger).(*slog.Logger)
	if !ok || l == nil {
		return slog.New(NoopHandler{})
	}
	return l
}

func contextWithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKeySlogger, l)
}

func logDebug(ctx context.Context, msg string, attrs ...slog.Attr) {
	loggerFromContext(ctx).LogAttrs(ctx, slog.LevelDebug, msg, attrs...)
}

func logError(ctx context.Context, err error, attrs ...slog.Attr) {
	loggerFromContext(ctx).LogAttrs(ctx, slog.LevelError, err.Error(), append(attrs, slog.Any("error", err))...)
}
