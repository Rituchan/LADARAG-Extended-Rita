def req = mockRequest.getRequest()
def q = { key ->
    def vals = req?.getParameterValues(key)
    return (vals != null && vals.length > 0) ? String.valueOf(vals[0]) : null
}

def available = q("available")
if (available == "true")  return "available_true"
if (available == "false") return "available_false"

def type = q("type")
if (type != null) type = type.toLowerCase()
if (type == "fire")    return "fire"
if (type == "police")  return "police"
if (type == "medical") return "medical"
if (type == "rescue")  return "rescue"
if (type == "hazmat")  return "hazmat"

def status = q("status")
if (status != null) status = status.toLowerCase()
if (status == "idle")        return "idle"
if (status == "responding")  return "responding"
if (status == "on-scene")    return "on-scene"
if (status == "returning")   return "returning"
if (status == "maintenance") return "maintenance"
if (status == "unavailable") return "unavailable"

def zoneId = q("zoneId")
if (zoneId != null) zoneId = zoneId.toUpperCase()
if (zoneId == "Z-CENTRO") return "by_zone_centro"
if (zoneId == "Z-NORD")   return "by_zone_nord"
if (zoneId == "Z-SUD")    return "by_zone_sud"
if (zoneId == "Z-EST")    return "by_zone_est"
if (zoneId == "Z-OVEST")  return "by_zone_ovest"

return "example_list"
