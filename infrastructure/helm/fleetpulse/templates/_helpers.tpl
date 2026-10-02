{{- define "fleetpulse.name" -}}{{ .Chart.Name }}{{- end }}
{{- define "fleetpulse.fullname" -}}{{ .Release.Name }}-{{ include "fleetpulse.name" . }}{{- end }}
