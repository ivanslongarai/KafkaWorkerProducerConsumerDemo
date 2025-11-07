namespace Consumer;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string AutoOffsetReset { get; set; } = "Earliest";
    public bool EnableAutoCommit { get; set; } = false;
    public bool AllowAutoCreateTopics { get; set; } = true;
}
