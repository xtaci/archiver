function getseconds(ts)
	return int64.new(ts):shr(22)
end
