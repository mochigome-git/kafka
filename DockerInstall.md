# Update system packages

sudo dnf update -y

# Install Docker from the default AL2023 repository

sudo dnf install -y docker

# Start the Docker service

sudo systemctl start docker

# Enable Docker to automatically run on boot

sudo systemctl enable docker

# Add your user to the docker group to run commands without 'sudo'

sudo usermod -aG docker $USER

# Create the global CLI plugins directory

sudo mkdir -p /usr/local/lib/docker/cli-plugins

# Download the latest Docker Compose binary matching your system architecture

sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m) -o /usr/local/lib/docker/cli-plugins/docker-compose

# Secure ownership and make the binary executable

sudo chown root:root /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Check Docker Engine status

docker --version

# Check Docker Compose version

docker compose version
