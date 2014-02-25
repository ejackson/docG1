setwd("~/work/Projects/playing/docgraph")

library(plyr)
library(parallel)
library(doParallel)
library(ggplot2)
library(functional)
library(lubridate)
library(igraph)

require(GGally)

registerDoParallel(cores=8)

# First read in the data
skygraph <- read.csv("./resources/sky-graph.csv",
                     header=FALSE,
                     col.names=c("from_id","from_addr","from_type","from_class",
                                 "from_speciality","from_sky","to_id","to_addr","to_type",
                                 "to_class","to_speciality","to_sky","refs"))

# This could be more easily done in Clojure
flux.type <- function(from, to) {
  out <- rep("external", length(from))
  out[from == ":not-sky-doc" & to == ":sky-doc"] <- "ingress"
  out[from == ":sky-doc" & to == ":sky-doc"]     <- "internal"
  out[from == ":sky-doc" & to == ":not-sky-doc"] <- "egress"
  out
}

# Add a column that classifies, ingress, egress, internal
skygraph <- transform(skygraph,
                      flux = flux.type(from_sky,
                                       to_sky))

# Now some simple analytics

# Bubble plot to see what's important to us
ingress <- subset(skygraph, flux=="ingress")
by.speciality <- ddply(ingress,
                       .(to_speciality),
                       plyr::here(summarise),
                       total_refs = sum(refs),
                       .parallel=TRUE)
by.speciality

ggplot(by.speciality, aes(x=to_speciality, y=total_refs)) + 
  geom_bar(position="dodge", stat="identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))

# Ingress by to-spec and from-spec
by.speciality <- ddply(ingress,
                       .(to_speciality, from_speciality),
                       plyr::here(summarise),
                       total_refs = sum(refs),
                       .parallel=TRUE)

ggplot(by.speciality, aes(x=to_speciality, y=total_refs)) + 
  geom_bar(position="dodge", stat="identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  facet_wrap (~ from_speciality)

# Top 10
print("INGRESS TOP 10 LIST")
print(head(by.speciality[order(by.speciality$total_refs, decreasing=TRUE),
                   c("from_speciality", "to_speciality", "total_refs")],
     n=10))


# Graph games
el <- ingress[, c("from_id","to_id")]
ell <- matrix(sapply(el, as.character), nc=2)
g <- graph.edgelist(ell)
ggnet(g,
      node.color=ingress$to_speciality,
      top8.nodes =TRUE,
)
gplot(ell)
# So, by speciality, lets see going in to Sky vs Not
by.speciality <- ddply(skygraph,
                       .(to_sky),
                       plyr::here(summarise),
                       total_refs = sum(refs),
                       .parallel=TRUE)

by.speciality.so <- subset(by.speciality, to_sky==":sky-doc")


g <- graph.ring(10)
g <- set.graph.attribute(g, "name", "RING")
# It is the same as
g$name <- "RING"
g$name

g <- igraph::set.vertex.attribute(g, "color", value=c("red", "green"))
igraph::get.vertex.attribute(g, "color")

g <- set.edge.attribute(g, "weight", value=runif(ecount(g)))
get.edge.attribute(g, "weight")

# The following notation is more convenient

g <- graph.star(10)

V(g)$color <- c("red", "green")
V(g)$color

E(g)$weight <- runif(ecount(g))
E(g)$weight

print(g, g=TRUE, v=TRUE, e=TRUE)


ggplot(by.speciality, aes(x=to_class, y=total_refs)) + 
  geom_bar(position="dodge") + theme(axis.text.x = element_text(angle = 90, hjust = 1))


by.speciality <- ddply(skygraph,
                       .(flux),
                       plyr::here(summarise),
                       total_refs = sum(refs),
                       .parallel=TRUE)
by.speciality

ggplot(by.speciality, aes(x=to_speciality, y=total_refs, size=total_refs)) + 
  geom_point(position="dodge") + theme(axis.text.x = element_text(angle = 90, hjust = 1))


# Consider only the flux across the boundaries of Sky.

# So who are the valuable docs ?
#  - Docs who's average $ in total chain is high
#  Target : high avg $ chain, small number of instances.

# Bubble plot to see what's important to us
egress <- subset(skygraph, flux=="egress")
egress.by.speciality <- ddply(egress,
                       .(to_speciality),
                       plyr::here(summarise),
                       total_refs = sum(refs),
                       .parallel=TRUE)

ggplot(egress.by.speciality, aes(x=to_speciality, y=total_refs)) + 
  geom_bar(position="dodge", stat="identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))

# Hmmm... can we narrow it down
egress.by.addr <- ddply(egress,
                              .(to_addr, to_speciality),
                              plyr::here(summarise),
                              total_refs = sum(refs),
                              .parallel=TRUE)

ggplot(egress.by.addr, aes(x=to_addr, y=total_refs, fill=to_speciality)) + 
  geom_bar(position="dodge", stat="identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))
