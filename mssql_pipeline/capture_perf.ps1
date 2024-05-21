# Capture total committed Bytes in use and CPU percentage while script is running

while($true){
    $cpuUsage = Get-CimInstance -Query "SELECT * FROM Win32_PerfFormatteddata_PerfOS_Processor WHERE name = '_Total'" | Select-Object PercentProcessorTime
    $memoryUsage = Get-CimInstance -Query "SELECT * FROM Win32_PerfFormatteddata_PerfOS_Memory" | Select-Object PercentCommittedBytesInUse
    if ($null -eq $cpuUsage.PercentProcessorTime) {
        $cpu = 999
    }
    else {
        $cpu = $cpuUsage.PercentProcessorTime
    }
    if ($null -eq $memoryUsage.PercentCommittedBytesInUse) {
        $output = 999
    }
    else {
        $memory = $memoryUsage.PercentCommittedBytesInUse
    }
    $output = "("+ $cpu +","+ $memory + ")"
    $output
}
