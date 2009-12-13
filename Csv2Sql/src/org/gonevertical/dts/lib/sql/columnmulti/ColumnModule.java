package org.gonevertical.dts.lib.sql.columnmulti;

import org.gonevertical.dts.lib.sql.columnlib.ColumnLib;
import org.gonevertical.dts.lib.sql.columnlib.MsSqlColumnLib;
import org.gonevertical.dts.lib.sql.columnlib.MySqlColumnLib;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

public class ColumnModule implements Module {

  @Override
  public void configure(Binder binder) {

    binder.bind(ColumnLib.class).annotatedWith(Names.named("MySql")).to(MySqlColumnLib.class);
    binder.bind(ColumnLib.class).annotatedWith(Names.named("MsSql")).to(MsSqlColumnLib.class);

  }

}