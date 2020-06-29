class ForceDirectedGraph {
  constructor(elem_id, width, height) {
    this.id = elem_id;
    this.width = width;
    this.height = height;

    this.svg = d3.select('#' + elem_id)
      .attr('width', width)
      .attr('height', height);

    this.canvas = this.svg.append('g');

    this.nodes = null;
    this.datahub = null;

    this.selectedCounter = 0;
    this.cat10 = d3.scale.category10().domain(d3.range(10));

    this.withImages = false;
    this.displayHint = true;

    this.minNodeRadius = 5;
    this.maxNodeRadius = 15;
    this.defaultLinkDistance = 50;
    this.imageWidth = 30;
    this.hintFontSize = '14px';

    this.distance = 100;
    this.charge = -70;
    this.force = d3.layout.force()
        .gravity(.08)
        .distance(this.distance)
        .charge(this.charge)
        .size([width, height]);

    this.canvas.append('g')
      .attr('class', 'links')
      .attr('stroke-width', .5)
      .attr('stroke-opacity', .5)
      .attr('stroke', 'black');

    this.canvas.append('g')
      .attr('class', 'nodes')
      .attr('stroke', 'white')
      .attr('stroke-width', 1.5)
      .attr('fill', 'gray');

    this.svg.call(d3.behavior.zoom()
      .on("zoom", () => {
        let velocity = 1/10;
        let scale =  Math.pow(d3.event.scale, velocity);
        let ty = (height - (height * scale))/2;
        let tx = (width - (width * scale))/2;
        this.canvas
          .attr("transform", "translate(" + [ty,tx] + ")scale(" + scale + ")");
      }))
      .on("mousedown.zoom", null)
      .on("touchstart.zoom", null)
      .on("touchmove.zoom", null)
      .on("touchend.zoom", null);
  }

  getNodes() {
    return this.canvas
      .select('g.nodes')
      .selectAll('g.node');
  }

  getLinks() {
    return this.canvas
      .select('g.links')
      .selectAll('line');
  }

  setData(newNodes, newLinks) {
    let self = this;
    self.force.stop();

    let oldNodes = self.getNodes().data();

    for (let i = newNodes.length - 1; i >= 0; i--) {
        let nn = newNodes[i];
        let on = oldNodes.find(function (d) {
            return d !== 'undefined' && nn.id == d.id;
        });

        if (on) {
            newNodes[i].x = on.x;
            newNodes[i].y = on.y;
            newNodes[i].selected = on.selected;
        }
    }

    // ------------- links

    let links = self.getLinks().data(newLinks);

    // update
    links.style('stroke-width', (d) => self.linkStrokeWidth(d));
    // remove
    links.exit().remove();
    // create
    links.enter().append('line')
      .style('stroke-width', (d) => self.linkStrokeWidth(d));

    // ---------------- nodes

    function onMouseover(d) {
        if (self.selectedCounter > 0 && !d.selected)
          d3.select(this).style('opacity', null);

        if (self.datahub != null && typeof self.datahub != 'undefined') {
          self.datahub.notifyMouseover(d, self);
        }
    }

    function onMouseout(d) {
        if (!d.selected && self.selectedCounter > 0)
          d3.select(this).style('opacity', .5);

        if (self.datahub != null && typeof self.datahub != 'undefined') {
          self.datahub.notifyMouseout(d, self);
        }
    }

    function onClick(d) {
        if (d.selected) self.selectedCounter -= 1;
        else self.selectedCounter += 1;

        d.selected = !d.selected;

        if (d.selected && self.selectedCounter == 1) {
          self.getNodes()
            .filter(function (n) { return !n.selected; })
            .style('opacity', .5);
        } else if (self.selectedCounter <= 0) {
            self.getNodes().style('opacity', null);
        }

        if (self.datahub != null && typeof self.datahub != 'undefined') {
          self.datahub.notifyMouseclick(d, self);
        }
    }

    let nodes = self.getNodes()
      .data(newNodes, self.nodeId);

    // remove.
    nodes.exit().remove();

    // create.
    let createdNodes = nodes.enter().append("g")
      .attr("class", "node")
      .style('opacity', (d) => self.nodeOpacity(d))
      .call(self.force.drag)
      .on('mouseover', onMouseover)
      .on('mouseout', onMouseout)
      .on('click', onClick);

    if (self.withImages) {
      createdNodes.append('image')
        .attr("xlink:href", (d) => self.nodeImage(d))
        .attr("width", (d) => self.imageWidth(d));
    } else {
      createdNodes.append('circle')
        .attr('r', (d) => self.nodeRadius(d))
        .attr('stroke', (d) => self.nodeStroke(d))
        .attr('fill', (d) => self.nodeFill(d));
    }

    if (this.displayHint) {
      createdNodes.append('text')
        .attr("dx", 15)
        .attr("dy", ".05em")
        .attr('stroke-width', .5)
        .style('font-size', self.hintFontSize)
        .text((d) => self.nodeHint(d));
    }

    // update.
    nodes.style('opacity', (d) => self.nodeOpacity(d));
    if (self.withImages) {
      nodes.select("image")
        .attr("xlink:href", (d) => self.nodeImage(d))
        .attr("width", self.imageWidth);
    } else {
      nodes.select('circle')
        .attr('r', (d) => self.nodeRadius(d))
        .attr('stroke', (d) => self.nodeStroke(d))
        .attr('fill', (d) => self.nodeFill(d));
    }

    if (self.displayHint) {
      nodes.select('text').text((d) => self.nodeHint(d));
    }

    if (self.selectedCounter > 0)
      nodes
        .filter((d) => { return !d.selected; })
        .style('opacity', '.3')

    // force-directed placement.
    self.force
      .nodes(newNodes)
      .links(newLinks)
      .linkDistance((d) => self.linkDistance(d))
      .alpha(.05)
      .on('tick', (t) => {
        links
            .attr('x1', (d) => { return d.source.x; })
            .attr('y1', (d) => { return d.source.y; })
            .attr('x2', (d) => { return d.target.x; })
            .attr('y2', (d) => { return d.target.y; });

        nodes.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
        // nodes.attr('transform', function (d) {
        //   let x = Math.max(d.radius, Math.min(self.width - d.radius, d.x));
        //   let y = Math.max(d.radius, Math.min(self.height - d.radius, d.y));
        //   return "translate(" + x + "," + y + ")";
        // });
      })
      .start();
  }

  linkStrokeWidth(d) {
    return d.width || null;
  }

  linkDistance(d) {
    let dist = d.distance || this.defaultLinkDistance;
    let sr = d.source.radius || this.minNodeRadius;
    let tr = d.target.radius || this.minNodeRadius;
    return dist + sr + tr;
  }

  nodeId(d) {
    return d.id || null;
  }

  nodeRadius(d) {
    return d.radius || self.minNodeRadius;
  }

  nodeFill(d) {
    if (typeof d.hsl != 'undefined') {
      return d3.hsl(d.hsl[0], d.hsl[1], d.hsl[2]);
    } else if (typeof d.fill != 'undefined' && d.fill >= 0) {
      return d3.hsl(d.fill, 1, 0.5);
    } else {
      return null;
    }
  }

  nodeStroke(d) {
    if (typeof d.stroke != 'undefined') {
      return d.stroke;
    } else if (typeof d.group != 'undefined' && d.group >= 0) {
      return this.cat10(d.group);
    } else {
      return null;
    }
  }

  nodeOpacity(d) {
    return d.opacity || null;
  }

  nodeHint(d) {
    return d.hint || null;
  }

  nodeImage(d) {
    return d.img || null;
  }

  watchBucket(bucket_id) {
    let self = this;
    let bucket = document.getElementById(bucket_id);
    let scope = angular.element(bucket).scope();

    scope.$watch(bucket_id, (newVal, oldVal) => {
        if (typeof newVal == 'undefined') {
            console.log('Undefined data');
        } else {
            try {
                let data = JSON.parse(newVal);
                self.setData(data.nodes, data.links);
            } catch (e) {
                console.log(e);
            }
        }
    });
  }

  listenUpdate(data) {
    this.setData(data.nodes, data.links);
  }

  listenSelected(selectedElems, caller) {
    let self = this;
    let nodes = self.getNodes();
    let data = selectedElems.data();
    let selectedNodes = nodes
      .filter(function(d) {
        for (let i = 0 ; i < data.length ; i++)
          if (data[i].id == d.id)
            return true;

        return false;
      });

    nodes.style('opacity', .3);
    selectedNodes.style('opacity', 1);
  }

  listen(hub) {
    this.datahub = hub;
    return this;
  }
}
