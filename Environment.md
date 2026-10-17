
##  Experimental Environment

### 1.1 Hardware

| Item | Specification |
|---|---|
| Operating System | Windows 11 家庭版 中文版 |
| CPU | Intel(R) Core(TM) Ultra 9 285H |
| RAM | 32.0 GB |
| Storage | 954 GB |

### 1.2 Software

| Software | Version | Environment |
|---|---|---|
| PostgreSQL | 18.6| Windows x86_64 |
| openGauss | 7.0.0-RC3 | Docker container (Linux x86_64) |
| Java JDK | 21.0.10 | Windows x86_64 |
| Docker Desktop | 4.90.0 | Windows x86_64 |
| VS Code | 1.141.0 | Windows x86_64 |

### 1.3 Database Configuration

**PostgreSQL**
- Deployment: Windows x86_64
- Version: 18.6

**openGauss**
- Deployment: Docker container
- Database version: 7.0.0-RC3
- Build ID: 01b7e318
- Container name: opengauss
- Container ID: 9b46233c3bf6
- Image: opengauss/opengauss-server:latest
- Image ID: d554be1b8b08
- Host port: 8888
- Container port: 5432
- Architecture: x86_64
- Compiler: GCC 10.3.0

**Notes**
 Hardware and container resource differences should be considered in performance comparisons.

## 2. Dataset

- Name: Film Database
- Source: `filmdb.sql`
- Original file: `data/original/filmdb.sql`
- Tables: movies
- Number of records:  9204
