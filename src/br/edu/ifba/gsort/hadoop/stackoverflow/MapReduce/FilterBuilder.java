package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce;

import java.util.ArrayList;
import java.util.List;

public class FilterBuilder {

	List<Filter> filtros = new ArrayList<Filter>();
			
	FilterBuilder add(Filter filtro){
		filtros.add(filtro);
		return this;
	}
	
	Boolean ok(){
		Boolean resultado = false;
		for (Filter filtro:filtros){
			if (!filtro.passIf()){
				return false;
			}
		}
		return resultado;
	}
	
}
