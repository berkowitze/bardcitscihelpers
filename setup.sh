pip3 install virtualenv
python3 -m virtualenv venv
source venv/bin/activate

pip install -r requirements.txt
echo ""
echo "Setup complete"

head -n 24 images.py  | tail -n 16
