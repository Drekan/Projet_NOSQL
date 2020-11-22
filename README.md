# Projet NoSQL

## TODO 
* Implémentation de la méthode générale de résolution de requête
* Merge seulement sur les variables projetées
* Implémenter le sortMergeJoin
* méthode starVariables(String query) qui renvoie les variables présentes dans tous les patterns
* optimiser la méthode de requête en étoile (intersection progressive)
* Dictionnaire : tester si une ressource demandée est connue avant de faire get
* Rendre effectives toutes les options (si export_query_stats est à **true**, on génère le fichier correspondant...)
* mettre en place des tests d'accès aux index
* calcul des statistiques, pouvoir les exporter
* gérer DataStructure > FileNotFoundException
* Comparer valuesNumber avec la taille du dictionnaire (les données contiennent-elles des redondances ?)
* Solveur : shuffle dans le constructeur?
* revoir un peu la logique de Dictionnaire
* améliorer verbose
* Système de mise en cache des DataStructure