using Mapster;

public static class MapsterConfig
{
    public static void Configure()
    {
        TypeAdapterConfig<string[], Modello>.NewConfig()
            .Map(dest => dest.Id, src => src.ElementAtOrDefault(0))
            .Map(dest => dest.Name, src => src.ElementAtOrDefault(1));
    }
}
