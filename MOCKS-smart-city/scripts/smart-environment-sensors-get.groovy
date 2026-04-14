def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def sensorType = q("sensorType")
if (sensorType != null) sensorType = sensorType.toLowerCase()
if (sensorType == "air_quality")  return "air_quality"
if (sensorType == "temperature")  return "temperature"
if (sensorType == "humidity")     return "humidity"
if (sensorType == "noise")        return "noise"
if (sensorType == "pressure")     return "pressure"
if (sensorType == "wind_speed")   return "wind_speed"
if (sensorType == "light")        return "light"

def alertActive = q("alertActive")
if (alertActive == "true")  return "alertActive_true"
if (alertActive == "false") return "alertActive_false"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"

return "example_list"
