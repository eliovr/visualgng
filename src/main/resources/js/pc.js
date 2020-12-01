class ParallelCoordinates {
  constructor(elem_id, features) {
    this.id = elem_id;
    this.datahub = null;

    this.features = features || [];
    this.filters = { lower: [], upper: [] };

    this.painted = false;

    this.height = 500;        // svg height. Width is defined based on the number of features.
    this.padding = {          // svg paddings.
        top: 10,
        bottom: 10,
        left: 10
    };
    this.axis_span = 100;      // distance between each axis.
    this.axis_width = 0;      // width of each axis (might vary based on boxplot_width).
    this.boxplot_width = 6;   // width of each box-plot.
    this.boxplot_margin = 1;
    // this.cat10 = d3.scale.category10().domain(d3.range(10));
    // this.cat20 = d3.scale.category20().domain(d3.range(20));
    let colors = ['#66c2a5','#fc8d62','#8da0cb','#e78ac3','#a6d854','#ffd92f',
      '#e5c494','#b3b3b3', '#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00',
      '#ffff33','#a65628','#f781bf', '#666666'];
    this.cat20 = (i) => {
      let c = i;
      while (c >= colors.length) {
        c -= colors.length;
      }
      return colors[c];
    }
    this.paint_boxplots = false;
    this.hasClusters = false;

    this.svg = d3.select('#' + elem_id);

    this.series_style = { 'stroke': 'gray', 'fill': 'none', 'stroke-opacity': '0.7' };
    this.ticks_line_style = { 'stroke': '#78797c', 'fill': 'none' };
    this.ticks_text_style = { 'stroke': 'none', 'fill': 'black', 'font-size': '12px' };
    this.axis_bg_style = { 'fill': 'darkgray', 'opacity': '.4', 'stroke': 'black' };
    this.axis_labels_style = { 'fill': '#999', 'font-size': '13px' };
    this.axis_filters_style = { 'cursor': 'move' };
    this.boxplot_style = { 'stroke': '#34363a' };
    this.boxplot_box_style = { 'fill-opacity': '.4' };
    this.boxplot_median_style = { 'stroke-width': '2' };

    // -- series
    this.svg.append('g')
      .attr('class', 'series')
      .style(this.series_style);

    // -- axis
    let axis = this.svg.append('g').attr('class', 'axis');

    // -- ticks
    axis.append('g')
      .attr('class', 'ticks')
      .style(this.ticks_line_style);

    // -- labels
    axis.append('g')
      .attr('class', 'labels')
      .style(this.axis_labels_style);

    // -- boxplots
    axis.append('g')
      .attr('class', 'box-plots')
      .style(this.boxplot_style);

    // -- filters
    let filters = axis.append('g')
      .attr('class', 'filters')
      .style(this.axis_filters_style);

    filters.append('g').attr('class', 'lower');
    filters.append('g').attr('class', 'upper');

    this.paint();
  }

  setFeatures(features) {
    this.features = features;
  }

  isFiltered(d) {
    let lower = this.filters.lower,
        upper = this.filters.upper;

    for (let i = 0; i < lower.length; i++) {
        const l = lower[i],
          u = upper[i],
          x = d.data[i];

        if (x > u.value || x < l.value) return true;
    }

    return false;
  }

  getSeries() {
    return this.svg.select('g.series').selectAll('path');
  }

  isNotFiltered(d) {
    return !this.isFiltered(d);
  }

  getNonFiltered() {
      return this.getSeries().filter((d) => this.isNotFiltered(d));
  }

  onFilter(elems) {
    if (this.datahub != null && typeof this.datahub != 'undefined') {
      this.datahub.notifySelected(elems.data(), this);
    }
  }

  getPath(d) {
    let self = this;
    let values = d.data;
    let features = self.features;
    let f = features[0];
    let first_y = f.scale(values[0]);
    let path = 'M' + self.padding.left + ' ' + first_y + ' L' + (f.x + self.axis_width) + ' ' + first_y;

    for (let i = 1; i < values.length; i++) {
      f = features[i];
      const y = f.scale(values[i]);
      path += ' L' + f.x + ' ' + y + ' L' + (f.x + self.axis_width) + ' ' + y;
    }

    return path;
  }

  getStroke(d) {
    let self = this;
    if (self.isFiltered(d)) return 'lightgray';

    if (typeof d.group != 'undefined' && d.group >= 0)
      return self.cat20(d.group);
    else if (typeof d.hsl != 'undefined' && d.hsl != null)
      return d.hsl;
        // return d3.hsl(d.hsl[0], d.hsl[1], d.hsl[2]);
    else if (typeof d.hue != 'undefined' && d.hue >= 0)
      return d3.hsl(d.hue, 1, .5);
    else return null;
  }

  paint() {
    let self = this;
    let features = self.features;
    let longest_label = 0;

    for (let i = 0; i < features.length; i++) {
      longest_label = Math.max(longest_label, features[i].name.length);
    }

    self.axis_width = 0;
    self.labels_height = longest_label * 5.5;
    self.labels_y = self.height - self.labels_height - self.padding.bottom;
    self.axis_height = self.labels_y - 15;

    for (let i = 0; i < features.length; i++) {
      const f = features[i];
      f.x = (i * (self.axis_span + self.axis_width)) + self.padding.left + .5;
      f.scale = d3.scale.linear()
        .domain([f.min, f.max])
        .range([self.axis_height, self.padding.top]);

      if (self.paint_boxplots) {
        for (let j = 0; j < f.clusters.length; j++) {
          f.clusters[j].x = f.x + (j * self.boxplot_width + self.boxplot_margin);
        }
      }
    }

    self.width = features.length * (self.axis_span + self.axis_width) + self.padding.left + (self.labels_height / 2);

    self.svg
      .attr("height", self.height)
      .attr("width", self.width)
      .style('min-width', self.width + 'px');

    self.paintAxis();
    self.paintLabels();
    self.paintFilters();
  }

  paintAxis() {
    let self = this;
    let features = self.features;
    let ticks = self.svg.select('g.axis g.ticks');

    ticks.selectAll('*').remove();

    for (let i = 0; i < features.length; i++) {
      let f = features[i];

      let tick = d3.svg.axis()
        .scale(f.scale)
        .orient('right')
        .ticks(5)
        .tickFormat(d3.format('s'));

      ticks
        .append('g')
        .attr('transform', 'translate(' + (f.x + self.axis_width + self.boxplot_margin) + ',0)')
        .call(tick);

      ticks.append('rect')
        .attr('x', f.x)
        .attr('y', self.padding.top)
        .attr('width', self.axis_width)
        .attr('height', self.axis_height - self.padding.top)
        .style(self.axis_bg_style);
    }

    ticks.selectAll('text').style(self.ticks_text_style);
    ticks.selectAll('.tick').style({ 'display': 'none' });

    self.svg
      .on('mouseover', () => {
          ticks.selectAll('.tick').style({ 'display': null })
      })
      .on('mouseout', () => {
          ticks.selectAll('.tick').style({ 'display': 'none' })
      });
  }

  paintLabels() {
    let self = this;
    let features = self.features;
    let labels = self.svg.select('g.axis g.labels')
      .selectAll('text')
      .data(features)
      .attr('x', (d) => { return d.x })
      .attr('y', self.labels_y)
      .attr('transform', (d) => {
          return 'rotate(45,' + d.x + ', ' + self.labels_y + ')';
      })
      .text((d) => { return d.name; });

    labels.enter().append('text')
      .attr('x', (d) => { return d.x })
      .attr('y', self.labels_y)
      .attr('transform', (d) => {
          return 'rotate(45,' + d.x + ', ' + self.labels_y + ')';
      })
      .text((d) => { return d.name; });
  }

  paintFilters() {
    let self = this;
    let features = self.features;
    let filters = self.svg.select('g.axis g.filters');
    let drag = d3.behavior.drag()
      .origin((d) => { return d; })
      .on("drag", function (d) {
        let y = Math.min(Math.max(d3.event.y, d.min_y), d.max_y);

        if (d.y !== y) {
          this.setAttribute('cy', y);
          d.y = y;
          d.value = d.scale(y);

          // update stroke colors.
          self.getSeries().attr('stroke', self.getStroke.bind(self));
          // trigger onfilter event.
          self.onFilter(self.getNonFiltered());
        }
      });

    self.filters.lower = features.map(function (d) {
      return {
        x: d.x + self.axis_width,
        y: self.axis_height,
        min_y: self.padding.top,
        max_y: self.axis_height,
        scale: d3.scale.linear()
            .domain([self.axis_height, self.padding.top])
            .range([d.min, d.max]),
        value: d.min
      };
    });

    self.filters.upper = features.map(function (d) {
      return {
        x: d.x + self.axis_width,
        y: self.padding.top,
        min_y: self.padding.top,
        max_y: self.axis_height,
        scale: d3.scale.linear()
          .domain([self.axis_height, self.padding.top])
          .range([d.min, d.max]),
        value: d.max
      };
    });

    let lower = filters.select('g.lower')
      .selectAll('circle')
      .data(self.filters.lower)
      .attr('cx', (d) => { return d.x; })
      .attr('cy', (d) => { return d.y; });

    lower.enter().append('circle')
      .attr('cx', (d) => { return d.x; })
      .attr('cy', (d) => { return d.y; })
      .attr('r', 3)
      .call(drag);

    let upper = filters.select('g.upper')
      .selectAll('circle')
      .data(self.filters.upper)
      .attr('cx', (d) => { return d.x; })
      .attr('cy', (d) => { return d.y; });

    upper.enter().append('circle')
      .attr('cx', (d) => { return d.x; })
      .attr('cy', (d) => { return d.y; })
      .attr('r', 3)
      .call(drag);
  }

  paintBoxplots(redraw) {
    let self = this;
    let features = self.features;
    if (typeof redraw === 'undefined')
      redraw = false;

    if (redraw)
      self.svg.select('g.axis g.box-plots')
        .selectAll('g')
        .remove();

    let axi = self.svg.select('g.axis g.box-plots')
      .selectAll('g')
      .data(features);

    // ----------- Boxes -----------
    if (self.hasClusters) {
      axi = (redraw ? axi.enter().append('g').attr('class', 'axi') : axi.selectAll('g.axi'))
        .selectAll('g')
        .data((d) => { return d.clusters; });
    }

    let box = (redraw ?
      axi.enter().append('g').attr('class', 'box-plot') :
      axi.selectAll('g.box-plot'));

    (redraw ? box.append('rect') : box.selectAll('rect'))
      .attr('x', (d) => { return d.x; })
      .attr('y', (d) => { return d.feature.scale(d.q3); })
      .attr('width', self.boxplot_width)
      .attr('height', (d) => { return d.feature.scale(d.q1) - d.feature.scale(d.q3); })
      .attr('fill', (d) => { return self.cat20(d.id); })
      .style(self.boxplot_box_style);

    // ----------- Medians --------
    (redraw ? box.append('line') : box.selectAll('line'))
      .attr('class', 'median')
      .attr('x1', (d) => { return d.x; })
      .attr('y1', (d) => { return d.feature.scale(d.median); })
      .attr('x2', (d) => { return d.x + self.boxplot_width; })
      .attr('y2', (d) => { return d.feature.scale(d.median); })
      .style(self.boxplot_median_style);

    // --------- Whiskers ---------
    (redraw ? box.append('path') : box.selectAll('path'))
      .attr('class', 'whisker')
      .attr('d', (d) => {
        let scale = d.feature.scale,
          lx = d.x,
          rx = lx + self.boxplot_width,
          mx = lx + (self.boxplot_width / 2),
          y1 = scale(d.w2),
          y2 = scale(d.q3),
          y3 = scale(d.q1),
          y4 = scale(d.w1);

        let path = 'M' + lx + ' ' + y1;
        path += ' H' + rx;
        path += ' M' + mx + ' ' + y1;
        path += ' V' + y2;
        path += ' M' + mx + ' ' + y3;
        path += ' V' + y4;
        path += ' M' + lx + ' ' + y4;
        path += ' H' + rx;

        return path;
      })
  }

  setData(data, redraw) {
    let self = this;

    let series = self.getSeries().data(data)
      .attr('stroke-width', null)
      .attr('stroke-opacity', null)
      .attr('stroke', (d) => self.getStroke(d))
      .attr('d', (d) => self.getPath(d));

    series.exit().remove();

    series.enter().append('path')
      .attr('d', (d) => self.getPath(d))
      .attr('stroke', (d) => self.getStroke(d));

    series
      .filter((d) => { return d.selected; })
      .attr('stroke-width', 3)
      .attr('stroke-opacity', 1);
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
          self.setData(data);
        } catch (e) {
          console.log(e);
        }
      }
    });
  }

  listen(hub) {
    this.datahub = hub;
    return this;
  }

  listenUpdate(data, caller) {
    this.setData(data.nodes);
  }

  listenMouseclick(elem, caller) {
    let line = this.getSeries().data()
      .find(function(d){ return d.id === elem.id; });

    if (line) line.selected = elem.selected;
  }

  listenMouseover(elem, caller) {
    let series = this.getSeries();

    series
      .filter((d) => { return !d.selected; })
      .attr('stroke-width', null)
      .attr('stroke-opacity', .25);

    series
      .filter((d) => { return d.id === elem.id; })
      .attr('stroke', (d) => this.getStroke(d))
      .attr('stroke-width', 3)
      .attr('stroke-opacity', 1);
  }

  listenMouseout(elem, caller) {
    let series = this.getSeries();
    let existsSelected = series.data().some(d => d.selected);

    if (existsSelected) {
      series.filter((d) => { return !d.selected; })
        .attr('stroke-width', null)
        .attr('stroke-opacity', .25);
    } else {
      series
        .attr('stroke', (d) => this.getStroke(d))
        .attr('stroke-width', null)
        .attr('stroke-opacity', null);
    }
  }
}
