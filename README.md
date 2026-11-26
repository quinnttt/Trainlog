# Trainlog

Online trips diary

## How to run the application

To set up the application, you need to provide certain configuration details. We use a YAML file for this purpose.

### Set up

[UV](https://docs.astral.sh/uv/) is recommended for managing your Python dependencies for this project. You'll also need to setup [Git LFS](https://git-lfs.com/).

```bash
git lfs install
git lfs pull
```

If you want to develop, install python dependencies into a virtual environment:

```bash
uv sync
```

Install pre-commit hooks, to automatically format and lint your code:

```bash
uv run pre-commit install
```

Copy the provided `config-example.yaml` and `.env.example`:

```bash
cp config-example.yaml config.yaml
cp .env.example .env
```

Edit the `.env` file with appropriate values.

Edit the `config.yaml` in your preferred text editor to fill in your details:

#### PostgreSQL configuration

This section is for the connection with the backend database:

- `user`: Database username
- `dbname`: Database name
- `hostname`: Database host
- `password`: Database password
- `port`: Database port

```yaml
pg:
  user: db_user
  dbname: your_database
  hostname: localhost
  password: db_password
  port: 5432
```

#### Owner Configuration

This section is for the application owner's details:

- `username`: Your preferred username.
- `email`: Your email address.
- `password`: Your password (will be securely hashed before storing).

```yaml
owner:
  username: your_username
  email: your_email@example.com
  password: your_password
```

### Run the app

Use Docker compose to start the app:
 
```bash
make start
```

Or if you prefer to run Flask outside of docker:

```bash
make start-local
```

### Important Notes

- **Security**: Ensure that the `config.yaml` file is secured with the right file permissions and is not publicly accessible. Keep it out of version control to prevent accidentally exposing sensitive details.
- **Placeholder Configuration**: Do not use the `config-example.yaml` as is for the application. Always create a copy and provide real values.
- **Supported Python version**: Currently Python 3.13.x
