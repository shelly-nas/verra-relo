#!/bin/bash

# Create virtual environment
echo "Creating virtual environment..."
python3.12 -m venv .venv

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

# Install Playwright browsers
echo "Installing Playwright browsers..."
playwright install chromium

echo "Setup complete!"
echo ""
echo "To activate the virtual environment, run:"
echo "source .venv/bin/activate"