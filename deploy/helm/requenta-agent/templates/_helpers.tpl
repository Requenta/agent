{{- define "agent.name" -}}
{{- printf "%s-agent" .Release.Name | trunc 55 | trimSuffix "-" -}}
{{- end -}}
{{- define "agent.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else if .Values.development -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- else -}}
{{- fail "Set image.digest to the verified digest of your built image; no public agent image is published yet" -}}
{{- end -}}
{{- end -}}
