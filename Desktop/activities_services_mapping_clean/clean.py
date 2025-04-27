import json
import io

# --- Mapping Data ---
# Service mapping data as a string
service_mapping_string = """
ID,Name
25,Fêtes d'anniversaire
27,Bébés bienvenus
28,Animaux bienvenus
29,Accessible en fauteuil
33,Réservation recommandée
35,Commande sans contact
39,Wi-Fi gratuit
40,Pas de Wi-Fi
41,Wi-Fi payant
43,Télévision
44,Prises à disposition
45,Téléphones interdits
46,Musique d'ambiance
47,Lieu calme
48,Karaoké
49,Spectacle vivant
50,Végétalisé
51,Associatif
52,Ecoresponsable
53,Zéro déchet
54,Escalier
55,Ascenseur
56,Vestiaire
57,Aquarium
58,accès Internet
59,Photos autorisées
60,Pas de photos
61,Baies vitrées
62,Parking gratuit
64,Parking vélos
65,Zone fumeur
67,Voiturier
68,Sommelier
70,Self service
71,Service vestiaire
72,Service de nettoyage
73,Service à table
74,Terrasse extérieure
75,Aire de pique nique
76,Terrain de jeux
77,Salle de sport
80,Bibliothèque
81,Jeux d'arcade
82,Billard français
83,Billard américain
86,Salle de projection
87,Piscine intérieure
88,Piscine extérieure
89,Plage
91,Galerie d'art
92,Chambres
102,Chaise de bébé
106,Poubelles
107,Extincteurs
108,Table à langer
109,Vue panoramique
110,Stationnement interdit
111,WC mixte
112,WC homme
113,WC femme
114,WC accessible en fauteuil
118,Gare à proximité
126,Jeux pour enfants
127,Jeux de société
128,Station de vélos à proximité
129,Cabaret
130,Antiquaire
131,Ouvert tard
132,Piste de danse
133,Salle de concert
134,Centre culturel
135,Temple bouddhiste
136,Magasin bio
137,Épicerie fine
138,Vidéo projecteur
139,Retransmissions sportives
140,"Pop-rock, populaire"
141,"Musique disco, dance"
142,"Latino, reggaeton"
143,"Jazz, blues"
144,"Musique vocal, instrumental"
145,"Musique électro, house, techno"
146,Musique classique
147,"Musique country, folk"
148,"Funk, soul, R&B, hiphop"
149,Parking sur la voie publique
150,Terrasse intérieure
151,Terrasse chauffée
152,Terrasse non chauffée
153,Terrasse couverte
154,Terrasse fumeur
155,Terrasse
156,Stationnement payant à proximité
157,Parking public à proximité
158,Vue exceptionnelle
159,Fumoir
160,Non-fumeur
161,Fumeur
162,Seulement à emporter
170,Bar à bière
174,Bar à thème
175,Circuit court
176,Vestiaire payant
199,Toilettes
200,Famille
"""

# Activity mapping data as a string
activity_mapping_string = """
ID  Name
1   Restaurants gastronomiques
2   Restauration rapide
3   Bistrot
4   Balade en forêt
5   Balade à cheval
6   Hôtel
7   Emplacement de camping
8   Chambre d'hôte
9   Gîte
10  Promenade en bateau
13  Lieu de culte
14  Escape Game
15  Laser Game
16  Cabaret
17  Danse
18  Théâtre
19  Bowling
20  Château
21  Atelier et cours créatif
22  Musique
23  Sensations
24  Sport aquatique
25  Sport collectif
26  Sport de glisse
27  Sport d'hiver
28  Sport d'équipe
29  Spa
30  Salle de sport
31  Brocante
32  Enchère
33  Foire
34  Salon
35  Marché
37  Stand de tir
38  Restaurant à thème
39  Sandwicherie
40  Grill house
41  Buffet
44  Zoos et parc
45  Piscine
46  Aire de jeux
48  Stade
49  Terrain multisports
50  Musée
51  Monument historique
54  Visite guidée
55  Concert
56  Galerie d'art
57  Centre culturel
58  Boutique de souvenirs
59  Boutique de mode
60  Magasin bio
61  Supermarché
62  Antiquaire
64  Maison de vente aux enchères
66  Marché artisanal
67  Artisan
68  Épicerie fine
69  Boulangerie
70  Café
71  Bar lounge
72  Bar à tapas
73  Bar à vin
74  Snack
75  Restaurant local
76  Restauration ambulante
77  Cuisine d'auteur
78  Zone de pique-nique
79  Restaurant routier
80  Brasserie
81  Restauration
82  Lieu ouvert la nuit
83  Bar
84  Club
85  Taxi
86  Location de vélo
87  Location de vélo électrique
88  Location de scooter
89  Location de scooter électrique
90  Location de voiture
91  Location de bateau
93  Distributeur de billets
94  Bureau de poste
95  Massage
96  Yoga
97  Manucure
98  Coiffeur
99  Esthéticienne
100 Physiothérapeute
101 Location de voiture électrique
102 Dentiste
103 Blanchisserie
104 Garage
105 Parking
106 Pharmacie
107 Plage aménagée
109 Randonnée
110 Plage
111 Médecin
112 Artisanat local
113 Marchand de légumes
114 Produits locaux
115 Poissonnier
116 Acrobranche
117 Parcours aventure
118 Ski alpin
119 Parc d'attractions
120 Animations
121 Avokart
122 Banane gonflable
123 Bateau barbecue
124 Biathlon
125 Surf sur les vagues
126 Navigation
127 Body surf
128 Bodyboard
129 Football bulle
130 Canoë
131 Kayak
132 Promenade en calèche
133 Équifeel
134 Équifun
135 Parcours d'obstacles de montagne
136 Equitation western
137 Catamaran
138 Plongée souterraine
139 Plongée enfants
140 Jouets de plage
141 Salle de cinéma
142 Escalade en salle
143 Escalade
144 Escalade à la corde
145 Alpinisme
146 Chasse sous-marine
147 Croisière
148 Course de chevaux
149 Courses de trot attelé
150 Trot
151 Ski de fond
152 Ski de randonnée
153 Curling
154 Disc golf
155 Parcours découverte
156 Bateau-dragon
157 Dressage
158 Promenade pédagogique
160 Plongée en apnée
161 Bateau électrique
162 Jet ski
163 Jet ski électrique
164 Motoneige électrique
165 Motoneige
166 Endurance
167 Équestre
168 Compétition équestre de cross-country
169 Tournée de concours complet
170 Écurie
171 Visite de la ferme
172 Nage avec palmes
173 Bateau de pêche
174 Parcours fitness
175 Flyboard
176 Surf sur foil
177 Visite gratuite
178 Plongée libre
179 Plongée avec les requins
180 Pêche
181 Saut à ski
182 Ski freeride
184 Freestyle ski
185 Baignade en eau douce
186 Natation en eau libre
187 Natation de compétition
188 Frisbee
189 Entraînement
190 Entraînement ludique
191 Golf
192 Mini golf
193 Toilettage
194 Guide de montagne
196 Randonnée
197 Plongée en groupe
198 Promenade à cheval avec un cheval mené à la main
199 Patrimoine
200 Visite du patrimoine
201 Hockey
202 Match de hockey
203 Tir à l'arc à cheval
204 Équitation
205 Randonnées à cheval
206 Horseball
207 Polo
208 Cheval de chasse
209 Hydrospeed
210 Plongée sous glace
211 Conduite sur glace
212 Patinage sur glace
213 Plongée individuelle
214 Golf en salle
215 Patinage sur glace en salle
216 Initiation à la plongée
217 Vol d'initiation en hélicoptère
218 Joëring
219 Kit volant
220 Kitesurf
221 Plongée en bateau
222 Plongée de nuit
223 Spéléologie
224 Kneeboarding
225 Paintball
226 Visite des producteurs locaux
227 Surf en longboard
228 Longboard skate
229 Méditation
230 Bateau à moteur
231 VTT
232 Luge de nuit
233 Traîneau à chiens
234 Marche nordique
235 Course d'orientation
236 Patinage en plein air
237 Stand-up paddle
238 Padel
239 Rafting
240 Yoga sur Stand-up paddle
241 Patinage au palais
242 Volley-ball
243 Football
244 Achats
245 Pilates
246 Aviron
247 Voilier
248 Châteaux de sable
249 Ski
250 Skimboard
251 Trampoline
252 Chasses au trésor
253 Paravoile
254 Parapente
255 Luge d'hiver
256 Via ferrata
257 Planche à voile
258 Ski nautique
259 Parc aquatique
260 Wakeboard
261 Rugby subaquatique
262 Hockey subaquatique
263 Pêche sportive
264 Balade à poney
265 Jeux de poney
266 Rallier
267 Scooter électrique
268 Scooter
269 Snowboard
270 Faire du vélo
271 Stand-up paddle sur la rivière
272 Rafting sur la rivière
273 Bateau de plaisance
274 Parc aquatique tropical
275 Gymnastique équestre
276 Scooter sous-marin
280 Voile
281 Plongée sur épave
282 Plongée sous-marine
283 Patinage à roulettes en salle
284 Triathlon
285 Jeu télévisé
286 Pêche en rivière
287 Promenade en mer
289 Promenade en traîneau
290 Surf sur matelas
291 Photographie sous-marine
292 Train touristique
293 Exploration des récifs
294 Motonautisme
295 Équitation de vitesse
296 Spartan race
297 Luge d'été
298 Spartan race d'hiver
299 Pédalos
300 Pédalos avec toboggan
301 Location de maillots de bain
302 Location de matériel de plongée
303 Location de ski
304 Location de patins à glace
305 Location de patins à roulettes
306 Chasse sous-marine récréative
307 Traîneau à chiens
308 Joering canin
309 Randonnée en raquettes
310 Jeu d'évasion sous-marine
311 Voiture buggy
313 Ultra trail
314 Exploration thématique
315 Ski en tandem
316 Bouée sur neige
317 Bouée sur l'eau
318 Vélo tandem
319 Montgolfière
320 Bouée tractée
321 Surf tracté
322 Course sur sentier
323 Techniques de Compétition de Randonnée Équestre
324 Baptême de parapente
325 Cours de ski
327 Aires de service
328 Station service
329 Stations-service n libre-service
330 Lavage de voiture
331 Station de recharge électrique
332 Loisirs
333 Bateau électrique
334 Location de bateaux électriques
335 Villes et villages
336 Visite de domaines
337 Visites dégustation
338 Visites découverte
339 Visites d'exception
340 Glacier
"""

# --- Create ID to Name Mappings ---

service_map = {}
service_lines = io.StringIO(service_mapping_string).readlines()
for line in service_lines:
    line = line.strip()
    if not line: continue # Skip empty lines
    try:
        # Split by the first comma
        parts = line.split(',', 1)
        # Explicitly check if the first part is the header string 'ID'
        if parts[0].strip() == 'ID':
            continue # Skip header row
        service_id = int(parts[0])
        service_name = parts[1].strip().strip('"') # Remove potential surrounding quotes
        service_map[service_id] = service_name
    except (ValueError, IndexError) as e:
        print(f"Warning: Could not parse service line: '{line}' - {e}")

activity_map = {}
activity_lines = io.StringIO(activity_mapping_string).readlines()
for line in activity_lines:
    line = line.strip()
    if not line: continue # Skip empty lines
    try:
        # Split by one or more whitespace characters, only once
        parts = line.split(maxsplit=1)
         # Explicitly check if the first part is the header string 'ID'
        if parts[0].strip() == 'ID':
            continue # Skip header row
        activity_id = int(parts[0])
        activity_name = parts[1].strip()
        activity_map[activity_id] = activity_name
    except (ValueError, IndexError) as e:
         print(f"Warning: Could not parse activity line: '{line}' - {e}")


# --- File Paths ---
input_file_path = r'C:\Users\victo\Desktop\activities_services_mapping_clean\activities_data.json' # Make sure your JSON file is named input.json
output_file_path = r'C:\Users\victo\Desktop\activities_services_mapping_clean\output.json' # This file will be created/overwritten

# --- Load JSON Data from File ---
try:
    with open(input_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"Successfully loaded data from {input_file_path}")
except FileNotFoundError:
    print(f"Error: Input file not found at {input_file_path}")
    exit()
except json.JSONDecodeError as e:
    print(f"Error decoding JSON from {input_file_path}: {e}")
    exit()
except Exception as e:
    print(f"An unexpected error occurred while reading the file: {e}")
    exit()

# --- Clean and Transform Data ---
cleaned_data = []

print("Starting data cleaning and transformation...")

for row in data:
    # Revised Filtering Logic: Keep row if 'name' is not null/empty AND 'activity' is a non-empty list
    name_is_valid = row.get('name') is not None and row.get('name') != ""
    activity_is_valid = isinstance(row.get('activity'), list) and len(row.get('activity')) > 0

    if name_is_valid and activity_is_valid:
        cleaned_row = row.copy() # Work on a copy

        # Transform activities (only if activity_is_valid is True, which it is for filtered rows)
        transformed_activities = []
        for activity_id in cleaned_row['activity']:
            if activity_id in activity_map:
                transformed_activities.append(activity_map[activity_id])
            else:
                print(f"Warning: Activity ID {activity_id} not found in mapping for row ID {cleaned_row.get('id', 'N/A')}. Skipping.")
        cleaned_row['activity'] = transformed_activities

        # Transform services (transform even if the original service list was empty, as per the original request)
        transformed_services = []
        if 'service' in cleaned_row and isinstance(cleaned_row['service'], list):
             for service_id in cleaned_row['service']:
                if service_id in service_map:
                    transformed_services.append(service_map[service_id])
                else:
                    print(f"Warning: Service ID {service_id} not found in mapping for row ID {cleaned_row.get('id', 'N/A')}. Skipping.")
        # Replace service list even if it was empty, it will become an empty list of names
        cleaned_row['service'] = transformed_services


        cleaned_data.append(cleaned_row)
    # else:
        # print(f"Removing row with ID {row.get('id', 'N/A')} due to invalid name or empty activity list.") # Uncomment to see why rows are removed


print(f"Finished cleaning. {len(cleaned_data)} rows kept out of {len(data)} original rows.")

# --- Write Cleaned Data to Output File ---
if cleaned_data:
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=4, ensure_ascii=False)
        print(f"Successfully wrote cleaned data to {output_file_path}")
    except IOError as e:
        print(f"Error writing to output file {output_file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while writing the file: {e}")
else:
    print("No valid data rows remained after cleaning. Output file will not be created.")