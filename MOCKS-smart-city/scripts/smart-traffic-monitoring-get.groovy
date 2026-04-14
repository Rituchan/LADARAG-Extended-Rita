def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def congestionLevel = q("congestionLevel")
if (congestionLevel != null) congestionLevel = congestionLevel.toLowerCase()
if (congestionLevel == "low")      return "low"
if (congestionLevel == "medium")   return "medium"
if (congestionLevel == "high")     return "high"
if (congestionLevel == "critical") return "critical"

def incidentDetected = q("incidentDetected")
if (incidentDetected == "true")  return "incidents_only"
if (incidentDetected == "false") return "no_incidents"

def active = q("active")
if (active == "true")  return "active_only"
if (active == "false") return "inactive_only"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
