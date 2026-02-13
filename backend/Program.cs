using System.Security.Claims;
using System.Text.Json;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Logging;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.AddSecurityDefinition("Bearer", new Microsoft.OpenApi.Models.OpenApiSecurityScheme
    {
        Name = "Authorization",
        Type = Microsoft.OpenApi.Models.SecuritySchemeType.Http,
        Scheme = "Bearer",
        BearerFormat = "JWT",
        In = Microsoft.OpenApi.Models.ParameterLocation.Header
    });

    c.AddSecurityRequirement(new Microsoft.OpenApi.Models.OpenApiSecurityRequirement
    {
        {
            new Microsoft.OpenApi.Models.OpenApiSecurityScheme
            {
                Reference = new Microsoft.OpenApi.Models.OpenApiReference
                {
                    Type = Microsoft.OpenApi.Models.ReferenceType.SecurityScheme,
                    Id = "Bearer"
                }
            },
            Array.Empty<string>()
        }
    });
});

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowFrontend",
        policy =>
        {
            policy.WithOrigins("http://localhost:3000")
                .WithExposedHeaders("Content-Disposition")
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials();
        });
});

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.Authority = builder.Configuration["Keycloak:Authority"];
        options.Audience = builder.Configuration["Keycloak:Audience"];
        options.MetadataAddress = $"{builder.Configuration["Keycloak:Chalenge"]}/.well-known/openid-configuration";
        options.RequireHttpsMetadata = bool.Parse(builder.Configuration["Keycloak:RequireHttpsMetadata"] ?? "false");
        options.BackchannelHttpHandler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true
        };

        options.TokenValidationParameters = new Microsoft.IdentityModel.Tokens.TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidateLifetime = true,
            ValidateAudience = false,
            ValidateIssuerSigningKey = true,
            ValidIssuer = builder.Configuration["Keycloak:Authority"],
            ValidAudience = builder.Configuration["Keycloak:Audience"],
            RoleClaimType = ClaimTypes.Role
        };

        options.Events = new JwtBearerEvents
        {
            OnAuthenticationFailed = context =>
            {
                Console.WriteLine($"Authentication failed: {context.Exception.Message}");
                return Task.CompletedTask;
            },
            OnTokenValidated = context =>
            {
                Console.WriteLine($"Token validated for user: {context.Principal?.Identity?.Name}");
                return Task.CompletedTask;
            }
        };
    });

builder.Services.AddAuthorization();

builder.Services.AddSingleton<ClickHouseConnection>(provider =>
{
    var config = provider.GetRequiredService<IConfiguration>();
    var connectionString = $"Host={config["ClickHouse:Host"]};Port={config["ClickHouse:Port"]};" +
                          $"Database={config["ClickHouse:Database"]};" +
                          $"User={config["ClickHouse:Username"]};" +
                          $"Password={config["ClickHouse:Password"]}";

    return new ClickHouseConnection(connectionString);
});

IdentityModelEventSource.ShowPII = true; // Показывает детальную информацию в исключениях
builder.Logging.AddFilter("Microsoft.AspNetCore.Authentication", LogLevel.Debug);

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors("AllowFrontend");
app.UseAuthentication();
app.UseAuthorization();

app.MapGet("/reports", async (ClaimsPrincipal user, ClickHouseConnection connection) =>
{
    try
    {
        var id = user.FindFirstValue(ClaimTypes.NameIdentifier);

        if (string.IsNullOrWhiteSpace(id) || !int.TryParse(id, out var userId))
        {
            return Results.Unauthorized();
        }

        await connection.OpenAsync();

        var query = @"
        SELECT 
            report_date,
            prosthesis_type,
            muscle_group,
            total_signals,
            avg_frequency,
            avg_duration,
            avg_amplitude
        FROM daily_prosthesis_reports
        WHERE user_id = @userId
        ";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.AddParameter("userId", userId);

        var reader = await cmd.ExecuteReaderAsync();
        var csv = new System.Text.StringBuilder();
        csv.AppendLine("Дата,Тип протеза,Мышечная группа,Всего сигналов,Ср. частота,Ср. длительность,Ср. амплитуда");

        while (await reader.ReadAsync())
        {
            csv.AppendLine(
                $"{reader.GetDateTime(0):yyyy-MM-dd}," +
                $"{reader.GetString(1)}," +
                $"{reader.GetString(2)}," +
                $"{reader.GetFieldValue<uint>(3)}," +
                $"{reader.GetFloat(4):F2}," +
                $"{reader.GetFloat(5):F2}," +
                $"{reader.GetFloat(6):F2}"
            );
        }

        return Results.File(
            System.Text.Encoding.UTF8.GetBytes(csv.ToString()),
            "text/csv",
            $"report_user-{userId}_{DateTime.Now:yyyyMMdd}.csv"
        );
    }
    catch (Exception ex)
    {
        return Results.Problem($"Ошибка при получении отчетов: {ex.Message}");
    }
}).RequireAuthorization();

app.Run();