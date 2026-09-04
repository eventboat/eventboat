These 18 YAML files are the official CloudEvents SQL TCK, vendored from the
cloudevents/sdk-go repository at tag v2.16.2 (sql/v2/test/tck/, Apache-2.0,
Copyright The CloudEvents Authors) — the copy of the suite the pinned parser
release is validated against. Two files (context_attributes_access.yaml,
not_operator.yaml) have drifted in the spec repository's main branch since
that release; the M3 acceptance rule is "100% of the official TCK for the
pinned SDK version", so upgrading the parser means re-vendoring from the
matching tag and re-reviewing those drifts. The runner (tck_test.go) mirrors
the SDK's own tck_test.go semantics: base event = test.FullEvent(), event /
eventOverrides application, error-type assertions, int expectations
normalized to int32.
