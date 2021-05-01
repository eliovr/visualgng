class ForceDirectedGraph {
  constructor(elem_id, features, custom_params) {
    this.id = elem_id;
    this.datahub = null;
    this.nodesData = [];
    this.linksData = [];
    this.features = features || [];

    let params = custom_params || {};

    this.width = params.width || 600;
    this.height = params.height || 600;

    this.selected = [];
    this.cat20 = d3.scale.category20().domain(d3.range(20));

    this.withImages = params.withImages || false;
    this.displayHint = true;

    this.minNodeRadius = 5;
    this.maxNodeRadius = 15;
    this.strokeWidth = 2.5;

    this.defaultLinkDistance = 30;
    this.minLinkDistance = 0;
    this.maxLinkDistance = 50;

    this.imageWidth = 30;
    this.hintFontSize = '14px';
    this.hintColor = 'black';

    this.distance = 100;
    this.charge = -70;
    this.force = d3.layout.force()
        .gravity(.08)
        .distance(this.distance)
        .charge(this.charge)
        .size([this.width, this.height]);

    let container = d3.select('#' + elem_id)
      .style({'min-height': this.height + 'px', 'min-width': this.width + 'px'});

    // --------------- Controls

    this.controls = container
      .append('div')
      .attr('class', 'controls btn-group-btn')
      .style({'position': 'relative', 'z-index': 10});

    this.controls.append('button')
      .attr('type', 'button')
      .attr('class', 'btn btn-default dropdown-toggle btn-sm')
      .attr('data-toggle', 'dropdown')
      .append('span')
        .text(' Graph')
        .attr('class', 'glyphicon glyphicon-cog');

    this.controls = this.controls.append('ul')
      .attr('class', 'dropdown-menu');

    // -- Display hint checkbox.
    this.controls.append('li')
      .attr('class', 'dropdown-header')
      .text('Show hint: ')
      .append('input')
        .attr('class', 'ml-2')
        .attr('type', 'checkbox')
        .property('checked', this.displayHint)
        .on('change', () => {
          this.displayHint = !this.displayHint;
          this.paint(false);
        })

    // -- Color by select.
    if (!this.withImages && this.features != []) {
      let options = [{'name': '-- Select --'}];
      for (var i = 0; i < this.features.length; i++) {
        options.push(this.features[i]);
      }

      let select = this.controls.append('li')
        .attr('class', 'dropdown-header')
        .text('Color by: ')
        .append('select')
          .attr('onclick', 'event.stopPropagation();')
          .on('change', () => {
            let i = d3.event.target.value;
            this.datahub.setSelectedFeature(i-1);
          })
          .selectAll('option')
          .data(options);

      select.enter().append('option')
        .attr('value', (d, i) => { return i; })
        .text((d) => { return d.name; })
    }

    // --------------- Canvas

    this.svg = container.append('svg')
      .attr('width', this.width)
      .attr('height', this.height)
      .style({'position': 'absolute', 'top': 0});

    this.canvas = this.svg.append('g');

    this.canvas.append('g')
      .attr('class', 'links')
      .attr('stroke-width', .5)
      .attr('stroke-opacity', .5)
      .attr('stroke', 'black');

    this.canvas.append('g')
      .attr('class', 'nodes')
      .attr('stroke', 'white')
      .attr('stroke-width', this.strokeWidth)
      .attr('fill', 'gray');

    this.svg.call(d3.behavior.zoom()
      .on("zoom", () => {
        let velocity = 1/10;
        let scale =  Math.pow(d3.event.scale, velocity);
        let ty = (this.height - (this.height * scale))/2;
        let tx = (this.width - (this.width * scale))/2;
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

  setData(data) {
    this.enrichData(data);
    this.nodesData = data.nodes;
    this.linksData = data.links;

    this.paint();
  }

  paint(runForce) {
    let self = this;
    let computeForce = typeof runForce == 'undefined' ? true : runForce;

    // ------------- links

    let links = self.getLinks().data(self.linksData);

    // update
    links.style('stroke-width', (d) => self.linkStrokeWidth(d));
    // remove
    links.exit().remove();
    // create
    links.enter().append('line')
      .style('stroke-width', (d) => self.linkStrokeWidth(d));


    // ---------------- nodes

    function onMouseover(d) {
        if (self.selected.length > 0 && !d.selected)
          d3.select(this).style('opacity', null);

        if (self.datahub != null && typeof self.datahub != 'undefined') {
          self.datahub.notifyMouseover(d, self);
        }
    }

    function onMouseout(d) {
        if (!d.selected && self.selected.length > 0)
          d3.select(this).style('opacity', .5);

        if (self.datahub != null && typeof self.datahub != 'undefined') {
          self.datahub.notifyMouseout(d, self);
        }
    }

    function onClick(d) {
        d.selected = !d.selected;
        self.selected = [];
        for (let i = 0 ; i < self.nodesData.length ; i++) {
            let node = self.nodesData[i];
            if (node.id == d.id) {
                node.selected = d.selected;
            }
            if (node.selected) {
                self.selected.push(node);
            }
        }

        if (d.selected && self.selected.length == 1) {
          self.getNodes()
            .filter((n)  => { return !n.selected; })
            .style('opacity', .5);
        } else if (self.selected.length == 0) {
            self.getNodes().style('opacity', null);
        }

        if (self.datahub != null && typeof self.datahub != 'undefined') {
            self.datahub.notifySelected(self.selected, self);
        }
    }

    let nodes = self.getNodes().data(self.nodesData, this.nodeId);

    // remove nodes.
    nodes.exit().remove();

    // update nodes.
    nodes.style('opacity', (d) => self.nodeOpacity(d));
    nodes.select('text')
      .text((d) => self.nodeHint(d))
      .style('display', self.displayHint ? null : 'none');

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

    if (self.selected.length > 0)
      nodes
        .filter((d) => { return !d.selected; })
        .style('opacity', '.3');

    // create nodes.
    let createdNodes = nodes.enter().append("g")
      .attr("class", "node")
      .style('opacity', (d) => self.nodeOpacity(d))
      .call(self.force.drag)
      .on('mouseover', onMouseover)
      .on('mouseout', onMouseout)
      .on('click', onClick);

    createdNodes.append('text')
      .attr("dx", 15)
      .attr("dy", ".05em")
      .attr('stroke-width', .5)
      .attr('fill', self.hintColor)
      .style('font-size', self.hintFontSize)
      .style('display', self.displayHint ? null : 'none')
      .text((d) => self.nodeHint(d));

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

    // ------------- force-directed placement.

    if (computeForce) {
      let minDistance = Number.MAX_SAFE_INTEGER;
      let maxDistance = Number.MIN_SAFE_INTEGER;

      for (var i = 0; i < self.linksData.length; i++) {
        const d = self.linksData[i].distance;
        minDistance = Math.min(d, minDistance);
        maxDistance = Math.max(d, maxDistance);
      }

      let distanceScaler = d3.scale.linear()
        .domain([minDistance, maxDistance])
        .range([self.minLinkDistance, self.maxLinkDistance]);

      self.force
        .nodes(self.nodesData)
        .links(self.linksData)
        .linkDistance((d) => self.linkDistance(d, distanceScaler))
        .alpha(.05)
        .on('tick', (t) => {
          links
              .attr('x1', (d) => { return d.source.x; })
              .attr('y1', (d) => { return d.source.y; })
              .attr('x2', (d) => { return d.target.x; })
              .attr('y2', (d) => { return d.target.y; });

          nodes.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
        })
        .start();
    }
  }

  linkStrokeWidth(d) {
    return d.width || null;
  }

  linkDistance(d, scaler) {
    let dist = d.distance || this.defaultLinkDistance;
    let sr = d.source.radius || this.minNodeRadius;
    let tr = d.target.radius || this.minNodeRadius;
    return scaler(dist) + sr + tr;
  }

  nodeId(d) {
    return d.id || null;
  }

  nodeRadius(d) {
    return d.radius || this.minNodeRadius;
  }

  nodeFill(d) {
    if (typeof d.hsl != 'undefined') {
      return d.hsl;
    } else if (typeof d.fill != 'undefined' && d.fill >= 0) {
      return d3.hsl(d.fill, 1, 0.5);
    } else if (typeof d.group != 'undefined' && d.group >= 0) {
      return this.cat20(d.group);
    } else {
      return null;
    }
  }

  nodeStroke(d) {
    if (typeof d.stroke != 'undefined') {
      return d.stroke;
    } else if (typeof d.group != 'undefined' && typeof d.hsl != 'undefined') {
      return this.cat20(d.group);
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

  enrichData(data) {
    let nodes = data.nodes;
    let links = data.links;
    let oldNodes = this.getNodes().data();
    let stats = {
      'min': Number.MAX_SAFE_INTEGER,
      'max': Number.MIN_SAFE_INTEGER
    }

    // ---- Nodes position.

    for (let i = nodes.length - 1; i >= 0; i--) {
        let n = nodes[i];
        let o = oldNodes.find((d) => {
            return d !== 'undefined' && n.id == d.id;
        });

        if (o) {
            n.x = o.x;
            n.y = o.y;
            n.selected = o.selected;
        }

        stats.min = Math.min(stats.min, n.density);
        stats.max = Math.max(stats.max, n.density);
    }

    // ---- Nodes size.

    let scaler = d3.scale.linear()
      .domain([stats.min, stats.max])
      .range([this.minNodeRadius, this.maxNodeRadius]);

    for (var i = 0; i < nodes.length; i++) {
      const d = nodes[i];
      d.radius = scaler(d.density);
    }

    // ---- Links distance.

    stats = {
      'min': Number.MAX_SAFE_INTEGER,
      'max': Number.MIN_SAFE_INTEGER
    }

    for (var i = 0; i < links.length; i++) {
      let x = links[i].distance;
      stats.min = Math.min(stats.min, x);
      stats.max = Math.max(stats.max, x);
    }

    scaler = d3.scale.linear()
      .domain([stats.min, stats.max])
      .range([this.minLinkDistance, this.maxLinkDistance]);

    for (var i = 0; i < links.length; i++) {
      const d = links[i];
      d.distance = scaler(d.distance);
    }
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
    if ('nodes' in data && 'links' in data) {
      this.setData(data);
    } else {
      console.warn("Data has no attribute 'nodes' or 'links'.");
    }

  }

  listenSelected(selectedElems, caller) {
    let nodes = this.getNodes();
    let nodesData = nodes.data();

    if (nodesData.length > selectedElems.length) {
      nodes.style('opacity', d => selectedElems.some(e => e.id === d.id) ? 1 : .3 );
    } else if (nodesData.some(d => d.selected)) {
      nodes.style('opacity', d => d.selected ? 1 : .3 );
    } else {
      nodes.style('opacity', 1);
    }
  }

  listen(hub) {
    this.datahub = hub;
    return this;
  }
}
