echo Mise à jour du serveur...
sudo apt-get update
echo Installation de pip...
sudo apt install pip -y
sudo pip install --upgrade pip -y
echo Installation des packages nécessaires...
pip install -r requirements.txt
touch cronlog.log

echo Installation finie vous pouvez maintenant commencer par modifier le fichier secret.json en ajoutant un sous compte avec ses clés api.