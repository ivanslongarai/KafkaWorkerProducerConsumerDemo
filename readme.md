# Kafka Demo .NET 8

This solution contains two .NET 8 worker services that demonstrate a simple Kafka producer–consumer architecture.

---

## 🧩 Overview

| Project | Role | Description |
|----------|------|-------------|
| **Producer** | Publisher | Sends random messages to a Kafka topic. |
| **Consumer** | Subscriber | Reads and logs messages from the same topic. |

Kafka runs in Docker using Confluent images, and both workers use **Serilog** for structured logging.

---

## 🐳 Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [.NET SDK 8.0](https://dotnet.microsoft.com/download/dotnet/8.0)

---

## 🚀 Run the Demo

### 1️⃣ Start Kafka and Zookeeper

In the project root:

bash
docker compose up -d
