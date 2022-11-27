package etl.utiles;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;


public class tiempo {
	private String formatoTime,formatoDate,formatoDateTime;
	
	public tiempo()
	{
		this.formatoTime="";
		this.formatoDate="";
		this.formatoDateTime="";
	}
	
	
	public String getPrimitiveDateTime()
	{
		return LocalDateTime.now().toString();
		
	}
	
	
	public String getPrimitiveDate()
	{
		return LocalDate.now().toString();
	}
	
	
	public String getPrimitiveTime()
	{
		return LocalTime.now().toString();
	}
	
	
	
	public String getDateTime()
	{
		return LocalDateTime.now().format(DateTimeFormatter.ofPattern(this.formatoDateTime));
		
	}
	
	
	public String getDate()
	{
		return LocalDate.now().format(DateTimeFormatter.ofPattern(this.formatoDate));
	}
	
	
	public String getTime()
	{
		return LocalTime.now().format(DateTimeFormatter.ofPattern(this.formatoTime));
	}
	
		public void setFormatDateTime(String patron)
	{
		this.formatoDateTime=patron;
	}
	
	
	public void setFormatDate(String patron)
	{
		this.formatoDate=patron;
	}
	
	
	public void setFormatTime(String patron)
	{
		this.formatoTime=patron;
	}
	
	
}
