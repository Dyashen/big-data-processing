# Kafka

Check out Learn Apache Kafka for Beginners.

## Transporting data

Source: Maakt data.
Target: Verbruikt data.

| w/ | wo/ |
| -- | -- |
|  Kafka functioneert hier als tussenpersoon. De tussenpersoon zal het verkeer naar de targets regelen. Zo moeten de sources niet verbonden zijn met alle targets. Dit zorgt voor een fouttolerant en veerkrachtig systeem. Kafka staat ook sterk bij horizontale schaalbaarheid. | Iedere source is verbonden met ieder target. Dit is de meest verbruikende manier van de twee. Hier moet iedere source rekening houden met protocollen, doorvoer, etc. |
| m + n | m x n|


## Topics:
Topics:
* Stream van data. Meerdere mogelijk.
* Naamgeving.
* Wordt opgedeeld in een **vast aantal partities**:

## Partities:
* Doorvoer verbeteren
* Een bestand op een lokaal FS.
* Append-only. Je kan enkel messages op het einde toevoegen.
* Offset = 0 : Allereerste bericht. 
* Offset van de laatste partitie = n
* Bij vergissing: pech!
  *  Je kan het niet verwijderen of aanpassen.
  *  Een dubbele actie (bv.: twee messages rond een aankoop): Je moet een derde message sturen om de dubbel ongedaan te maken.
* Ordening is niet gesorteerd!
  * min -> max
* Je kan het aanpassen (?), maar er hangen hier nadelen aan.


## Kenmerken
* Immutable
* Limited: Volgens de default policy worden berichten ouder dan een week verwijderd.
* Je specifieert de topic waar het bericht naartoe moet, niet de partition.
  * Partitie is willekeurig --> Load-balancing


## Broker
= Computer

* Elke broker een ID geven.
* Elk bestaat uit partities 
* --> weinig controle over de brokers: geen master/slave verhouding.
* Als je één broker kent, dan kan je verbinding maken met alles binnen de cluster.
* Per standaard: drie brokers.
* één broker ook mogelijk: geen schaalbaarheid.

Partitie toekennen aan broker(s):
* Algoritme

## Data replication
Het repliceren van data:
* Fouttolerantie: voorkomen dat data verloren raakt als het systeem van een partitie defunct gaat.
* Partitioneren verhoogt de schade bij een fout.

Replication factor:
* Factor hoger dan één, maar niet te hoog!
* Broker kapot --> andere broker bezit de data

Leader/followers:
* Leaders hebben volledige toegang tot de data.
  * Moet worden aangesproken als er iets in de partitie moet worden veranderd.
* Volgers hebben geen toegang. 
* Gedrag kan worden beïnvloed.
  * Een applicatie binnen dezelfde rack als een follower, van de data dat die nodig heeft, zal de volger aanspreken i.p.v. leader.

**Out-of-sync**: De volger beschikt niet meer over de meeste recente data.

**Fetch requests**: Geef mij alles dat begint vanaf deze offset.
  * De replica weet hoeveel offsets die achterloopt op de leider.
    * **In-sync**: er is geen verschil tussen de replica en de leider. Enkel zij komen in aanmerking om leider te worden.
  * Als de leider geen fetch request ziet voor meer dan 10 seconden == Out-of-Sync (Mortis)

**Producer**
* Schrijft/verstuurt data naar de topic(s).
* Zal automatisch opnieuw proberen.

| Hoe achterhalen of een bericht is toegekomen: | -- | |
| -- | -- | -- |
| Bericht versturen + schietgebedje | ack=0 | Unreliable, maar snel. |
| Bericht versturen + wachten op bevestiging | acks=1 | Deels geruststellend. |
| Bericht versturen en wachten tot de leider + in-sync replicas het bericht hebben ontvangen. | acks=all | Volledige geruststelling, maar gevaarlijk als er géén enkele replica in-sync is. Geen in-sync replicas: enkel de leader wordt geüpdatet. Als er géén in-sync replica's zijn is alles *'goed'* verlopen. Dit voorkom je door *min in-sync replica's* op twee te plaatsen. Nooit hetzelfde getal als je replication factor (vb.: 2 & 2), want dan verwacht je dat je geen trage volgers hebt. |

## Consumers:
* Data lezen van de topic(s)
* Toekennen aan partitie:
  * Speciale topic binnen Kafka: Consumer offset.


Consumer offset:
* De staat van topics.
* Logboek: "ik heb de messages t.e.m. 50 gelezen". 
* Achterhalen vanaf waar de consumer berichten moet verwerken.

## Delivery Semantics
* "Wat kan een consumer doen om het bericht te verwerken?"
* "Wanneer vertel je Kafka dat je klaar bent met het verwerken van een bericht?"


## Zookeeper
* leader-follower architecture


# Labo

We moeten hier het poortnummer 19092 gebruiken voor Kafka1. 

```cmd
kafka-topics --bootstrap-server kafka1:19092 --list
kafka-topics --bootstrap-server kafka2:19093 --list
kafka-topics --bootstrap-server kafka3:19094 --list
```

Het maakt niet uit bij welke broker. De actie zal altijd werken.

Aanmaken:
```cmd
kafka-topics --create --topic lecture --partitions 3 --replication-factor 3
```

Omschrijven:
```cmd
kafka-topics --bootstrap-server kafka1:19092 --describe --topic lecture
```

Nieuwe messages toevoegen:
```cmd
kafka-console-producer --bootstrap-server kafka1:19092 --topic-lecture
```
 
